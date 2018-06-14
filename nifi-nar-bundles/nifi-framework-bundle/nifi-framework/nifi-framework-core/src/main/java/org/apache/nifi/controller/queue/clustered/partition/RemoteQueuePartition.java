/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.controller.queue.clustered.partition;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.queue.BlockingSwappablePriorityQueue;
import org.apache.nifi.controller.queue.DropFlowFileRequest;
import org.apache.nifi.controller.queue.FlowFileQueueContents;
import org.apache.nifi.controller.queue.LoadBalancedFlowFileQueue;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.queue.clustered.TransferFailureDestination;
import org.apache.nifi.controller.queue.clustered.client.async.AsyncLoadBalanceClientRegistry;
import org.apache.nifi.controller.queue.clustered.client.async.TransactionCompleteCallback;
import org.apache.nifi.controller.queue.clustered.client.async.TransactionFailureCallback;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.RepositoryRecord;
import org.apache.nifi.controller.repository.StandardRepositoryRecord;
import org.apache.nifi.controller.repository.SwapSummary;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.provenance.ProvenanceEventBuilder;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RemoteQueuePartition implements QueuePartition {
    private static final Logger logger = LoggerFactory.getLogger(RemoteQueuePartition.class);

    private final NodeIdentifier nodeIdentifier;
    private final BlockingSwappablePriorityQueue priorityQueue;
    private final LoadBalancedFlowFileQueue flowFileQueue;
    private final TransferFailureDestination failureDestination;

    private final FlowFileRepository flowFileRepo;
    private final ProvenanceEventRepository provRepo;
    private final ContentRepository contentRepo;
    private final AsyncLoadBalanceClientRegistry clientRegistry;

    private boolean running = false;
    private final String description;

    public RemoteQueuePartition(final NodeIdentifier nodeId, final BlockingSwappablePriorityQueue priorityQueue, final TransferFailureDestination failureDestination,
                                final FlowFileRepository flowFileRepo, final ProvenanceEventRepository provRepo, final ContentRepository contentRepository,
                                final AsyncLoadBalanceClientRegistry clientRegistry, final LoadBalancedFlowFileQueue flowFileQueue) {

        this.nodeIdentifier = nodeId;
        this.priorityQueue = priorityQueue;
        this.flowFileQueue = flowFileQueue;
        this.failureDestination = failureDestination;
        this.flowFileRepo = flowFileRepo;
        this.provRepo = provRepo;
        this.contentRepo = contentRepository;
        this.clientRegistry = clientRegistry;
        this.description = "RemoteQueuePartition[queueId=" + flowFileQueue.getIdentifier() + ", nodeId=" + nodeIdentifier + "]";
    }

    @Override
    public QueueSize size() {
        return priorityQueue.size();
    }

    @Override
    public String getSwapPartitionName() {
        return nodeIdentifier.getId();
    }

    @Override
    public Optional<NodeIdentifier> getNodeIdentifier() {
        return Optional.ofNullable(nodeIdentifier);
    }

    @Override
    public void put(final FlowFileRecord flowFile) {
        priorityQueue.put(flowFile);
    }

    @Override
    public void putAll(final Collection<FlowFileRecord> flowFiles) {
        priorityQueue.putAll(flowFiles);
    }

    @Override
    public void dropFlowFiles(final DropFlowFileRequest dropRequest, final String requestor) {
        priorityQueue.dropFlowFiles(dropRequest, requestor);
    }

    @Override
    public SwapSummary recoverSwappedFlowFiles() {
        return priorityQueue.recoverSwappedFlowFiles();
    }

    @Override
    public FlowFileQueueContents packageForRebalance(String newPartitionName) {
        return priorityQueue.packageForRebalance(newPartitionName);
    }

    @Override
    public void setPriorities(final List<FlowFilePrioritizer> newPriorities) {
        priorityQueue.setPriorities(newPriorities);
    }

    // TODO: Test Expired FlowFiles
    private void handleExpired(final Set<FlowFileRecord> expired) {
        if (expired.isEmpty()) {
            return;
        }

        logger.info("{} {} FlowFiles have expired and will be removed", new Object[] {this, expired.size()});
        final List<RepositoryRecord> expiredRecords = new ArrayList<>(expired.size());
        final List<ProvenanceEventRecord> provenanceEvents = new ArrayList<>(expired.size());

        for (final FlowFileRecord flowFile : expired) {
            final StandardRepositoryRecord record = new StandardRepositoryRecord(flowFileQueue, flowFile);
            record.markForDelete();
            expiredRecords.add(record);

            final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder()
                    .fromFlowFile(flowFile)
                    .setEventType(ProvenanceEventType.EXPIRE)
                    .setDetails("Expiration Threshold = " + flowFileQueue.getFlowFileExpiration())
                    .setEventTime(System.currentTimeMillis());

            final ContentClaim contentClaim = flowFile.getContentClaim();
            if (contentClaim != null) {
                final ResourceClaim resourceClaim = contentClaim.getResourceClaim();
                builder.setCurrentContentClaim(resourceClaim.getContainer(), resourceClaim.getSection(), resourceClaim.getId(),
                        contentClaim.getOffset() + flowFile.getContentClaimOffset(), flowFile.getSize());

                builder.setPreviousContentClaim(resourceClaim.getContainer(), resourceClaim.getSection(), resourceClaim.getId(),
                        contentClaim.getOffset() + flowFile.getContentClaimOffset(), flowFile.getSize());
            }

            final ProvenanceEventRecord provenanceEvent = builder.build();
            provenanceEvents.add(provenanceEvent);

            final long flowFileLife = System.currentTimeMillis() - flowFile.getEntryDate();
            logger.info("{} terminated due to FlowFile expiration; life of FlowFile = {} ms", new Object[] {flowFile, flowFileLife});
        }

        try {
            flowFileRepo.updateRepository(expiredRecords);

            for (final RepositoryRecord expiredRecord : expiredRecords) {
                contentRepo.decrementClaimantCount(expiredRecord.getCurrentClaim());
            }

            provRepo.registerEvents(provenanceEvents);
        } catch (IOException e) {
            logger.warn("Encountered {} expired FlowFiles but failed to update FlowFile Repository. This FlowFiles may re-appear in the queue after NiFi is restarted and will be expired again at " +
                    "that point.", expiredRecords.size(), e);
        }
    }

    private FlowFileRecord getFlowFile() {
        final Set<FlowFileRecord> expired = new HashSet<>();
        final FlowFileRecord flowFile = priorityQueue.poll(expired, flowFileQueue.getFlowFileExpiration(TimeUnit.MILLISECONDS));
        handleExpired(expired);
        return flowFile;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }

        final TransactionFailureCallback failureCallback = new TransactionFailureCallback() {
            @Override
            public void onTransactionFailed(final List<FlowFileRecord> flowFiles, final Exception cause, final TransactionPhase phase) {
                if (phase == TransactionPhase.CONNECTING) {
                    failureDestination.putAll(priorityQueue::packageForRebalance);
                } else {
                    failureDestination.putAll(flowFiles);
                }
            }
        };

        final TransactionCompleteCallback successCallback = new TransactionCompleteCallback() {
            @Override
            public void onTransactionComplete(final List<FlowFileRecord> flowFilesSent) {
                // We've now completed the transaction. We must now update the repositories and "keep the books", acknowledging the FlowFiles
                // with the queue so that its size remains accurate.
                // TODO: Handle FlowFiles with ContentNotFound / "aborted records"!!
                updateRepositories(flowFilesSent, Collections.emptyList());
                priorityQueue.acknowledge(flowFilesSent);
                flowFileQueue.onTransfer(flowFilesSent);
            }
        };

        // TODO: Need to cleanup some stuff.... do we even need a BlockingSwappablePriorityQueue with this approach??
        clientRegistry.register(flowFileQueue.getIdentifier(), nodeIdentifier, priorityQueue::isEmpty, this::getFlowFile, failureCallback, successCallback);

        running = true;
    }


    /**
     * Updates the FlowFileRepository, Provenance Repository, and claimant counts in the Content Repository.
     *
     * @param flowFilesSent the FlowFiles that were sent to another node.
     * @param abortedRecords the Repository Records for any FlowFile whose content was missing.
     */
    private void updateRepositories(final List<FlowFileRecord> flowFilesSent, final Collection<RepositoryRecord> abortedRecords) {
        // We update the Provenance Repository first. This way, even if we restart before we update the FlowFile repo, we have the record
        // that the data was sent in the Provenance Repository. We then update the content claims and finally the FlowFile Repository. We do it
        // in this order so that when the FlowFile repo is sync'ed to disk, we know which Content Claims are no longer in use. Updating the FlowFile
        // Repo first could result in holding those Content Claims on disk longer than we need to.
        //
        // Additionally, we are iterating over the FlowFiles sent multiple times. We could refactor this to iterate over them just once and then
        // create the Provenance Events and Repository Records in a single pass. Doing so, however, would mean that we need to keep both collections
        // of objects in heap at the same time. Using multiple passes allows the Provenance Events to be freed from heap by the GC before the Repo Records
        // are ever created.
        final List<ProvenanceEventRecord> provenanceEvents = new ArrayList<>(flowFilesSent.size() * 2 + abortedRecords.size());
        for (final FlowFileRecord sent : flowFilesSent) {
            // TODO: Should discuss with others -- do we want SEND/DROP event for each FlowFile here? Or a "TRANSFER" event? Or nothing because
            // it is staying within the comfy confines of a NiFi cluster??
            provenanceEvents.add(createSendEvent(sent));
            provenanceEvents.add(createDropEvent(sent));
        }

        for (final RepositoryRecord abortedRecord : abortedRecords) {
            final FlowFileRecord abortedFlowFile = abortedRecord.getCurrent();
            provenanceEvents.add(createDropEvent(abortedFlowFile, "Content Not Found"));
        }

        provRepo.registerEvents(provenanceEvents);

        // Update the FlowFile Repository & content claim counts last
        final List<RepositoryRecord> flowFileRepoRecords = flowFilesSent.stream()
                .map(this::createRepositoryRecord)
                .collect(Collectors.toCollection(ArrayList::new));

        flowFileRepoRecords.addAll(abortedRecords);

        // Decrement claimant count for each FlowFile.
        flowFileRepoRecords.stream()
                .map(RepositoryRecord::getCurrentClaim)
                .forEach(contentRepo::decrementClaimantCount);

        try {
            flowFileRepo.updateRepository(flowFileRepoRecords);
        } catch (final Exception e) {
            logger.error("Unable to update FlowFile repository to indicate that {} FlowFiles have been transferred to {}. "
                    + "It is possible that these FlowFiles will be duplicated upon restart of NiFi.", flowFilesSent.size(), getNodeIdentifier(), e);
        }
    }

    private RepositoryRecord createRepositoryRecord(final FlowFileRecord flowFile) {
        final StandardRepositoryRecord record = new StandardRepositoryRecord(flowFileQueue, flowFile);
        record.markForDelete();
        return record;
    }

    private ProvenanceEventRecord createSendEvent(final FlowFileRecord flowFile) {

        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder()
                .fromFlowFile(flowFile)
                .setEventType(ProvenanceEventType.SEND)
                .setDetails("Re-distributed for Load-balanced connection")
                .setComponentId(flowFileQueue.getIdentifier())
                .setComponentType("Connection")
                .setSourceQueueIdentifier(flowFileQueue.getIdentifier())
                .setSourceSystemFlowFileIdentifier(flowFile.getAttribute(CoreAttributes.UUID.key()))
                .setTransitUri("nifi:connection:" + flowFileQueue.getIdentifier());

        final ContentClaim contentClaim = flowFile.getContentClaim();
        if (contentClaim != null) {
            final ResourceClaim resourceClaim = contentClaim.getResourceClaim();
            builder.setCurrentContentClaim(resourceClaim.getContainer(),resourceClaim.getSection() ,resourceClaim.getId(),
                    contentClaim.getOffset() + flowFile.getContentClaimOffset(), flowFile.getSize());

            builder.setPreviousContentClaim(resourceClaim.getContainer(),resourceClaim.getSection() ,resourceClaim.getId(),
                    contentClaim.getOffset() + flowFile.getContentClaimOffset(), flowFile.getSize());
        }

        final ProvenanceEventRecord sendEvent = builder.build();

        return sendEvent;
    }

    private ProvenanceEventRecord createDropEvent(final FlowFileRecord flowFile) {
        return createDropEvent(flowFile, null);
    }

    private ProvenanceEventRecord createDropEvent(final FlowFileRecord flowFile, final String details) {
        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder()
                .fromFlowFile(flowFile)
                .setEventType(ProvenanceEventType.DROP)
                .setDetails(details)
                .setComponentId(flowFileQueue.getIdentifier())
                .setComponentType("Connection")
                .setSourceQueueIdentifier(flowFileQueue.getIdentifier());

        final ContentClaim contentClaim = flowFile.getContentClaim();
        if (contentClaim != null) {
            final ResourceClaim resourceClaim = contentClaim.getResourceClaim();
            builder.setCurrentContentClaim(resourceClaim.getContainer(),resourceClaim.getSection() ,resourceClaim.getId(),
                    contentClaim.getOffset() + flowFile.getContentClaimOffset(), flowFile.getSize());

            builder.setPreviousContentClaim(resourceClaim.getContainer(),resourceClaim.getSection() ,resourceClaim.getId(),
                    contentClaim.getOffset() + flowFile.getContentClaimOffset(), flowFile.getSize());
        }

        final ProvenanceEventRecord dropEvent = builder.build();

        return dropEvent;
    }


    @Override
    public synchronized void stop() {
        running = false;
        clientRegistry.unregister(flowFileQueue.getIdentifier(), nodeIdentifier);
    }

    @Override
    public String toString() {
        return description;
    }
}
