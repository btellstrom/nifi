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
package org.apache.nifi.web.api;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.ComponentAuthorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.ResumeFlowException;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.concurrent.AsyncRequestManager;
import org.apache.nifi.web.api.concurrent.AsynchronousWebRequest;
import org.apache.nifi.web.api.concurrent.RequestManager;
import org.apache.nifi.web.api.concurrent.StandardAsynchronousWebRequest;
import org.apache.nifi.web.api.dto.AffectedComponentDTO;
import org.apache.nifi.web.api.dto.DtoFactory;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterContextUpdateRequestDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.Entity;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterContextUpdateRequestEntity;
import org.apache.nifi.web.util.AffectedComponentUtils;
import org.apache.nifi.web.util.CancellableTimedPause;
import org.apache.nifi.web.util.ComponentLifecycle;
import org.apache.nifi.web.util.LifecycleManagementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;


// TODO: Update FlowFromDOMFactory
// TODO: Update FlowSynchronizer
// TODO: Update Fingerprint
// TODO: Add validation/verification
// TODO: Replicate Request to update Parameter Context if clustered
// TODO: Add Policies
// TODO: Check Permissions
// TODO: Add to VersionedFlowSnapshot
// TODO: Update StandardProcessGroup when importing/updating a flow
// TODO: Update Add endpoint for validating components with a given configuration
// TODO: Templates
// TODO: Test all of this in cluster
// TODO: If processor referencing parameter but no context, do not throw Exception in setProperties() but instead just make processor invalid.

@Path("/parameter-contexts")
@Api(value = "/parameter-contexts", description = "Endpoint for managing version control for a flow")
public class ParameterContextResource extends ApplicationResource {
    private static final Logger logger = LoggerFactory.getLogger(ParameterContextResource.class);

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;
    private DtoFactory dtoFactory;
    private ComponentLifecycle clusterComponentLifecycle;
    private ComponentLifecycle localComponentLifecycle;

    private RequestManager<ParameterContextEntity> requestManager = new AsyncRequestManager<>(100, TimeUnit.MINUTES.toMillis(1L), "Parameter Context Update Thread");


    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
        value = "Create a Parameter Context",
        response = ParameterContextEntity.class,
        authorizations = {
            @Authorization(value = "Write - /parameter-contexts")
        })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response createParameterContext(
        @ApiParam(value = "The Parameter Context.", required = true) final ParameterContextEntity requestEntity) {

        if (requestEntity == null || requestEntity.getComponent() == null) {
            throw new IllegalArgumentException("Parameter Context must be specified");
        }

        if (requestEntity.getRevision() == null || (requestEntity.getRevision().getVersion() == null || requestEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new Parameter Context.");
        }

        final ParameterContextDTO context = requestEntity.getComponent();
        if (context.getName() == null) {
            throw new IllegalArgumentException("Parameter Context's Name must be specified");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
            serviceFacade,
            requestEntity,
            lookup -> {
                // TODO: Authorize
            },
            () -> {
                serviceFacade.verifyCreateParameterContext(requestEntity.getComponent());
            },
            entity -> {
                final String contextId = generateUuid();
                entity.getComponent().setId(contextId);

                final Revision revision = getRevision(entity.getRevision(), contextId);
                final ParameterContextEntity contextEntity = serviceFacade.createParameterContext(revision, entity.getComponent());

                // generate a 201 created response
                final String uri = generateResourceUri("parameter-contexts", contextEntity.getId());
                contextEntity.setUri(uri);
                return generateCreatedResponse(URI.create(uri), contextEntity).build();
            });
    }


    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @ApiOperation(
        value = "Modifies a Parameter Context",
        response = ParameterContextEntity.class,
        notes = "This endpoint will update a Parameter Context to match the provided entity. However, this request will fail if any component is running and is referencing a Parameter in the " +
            "Parameter Context. Generally, this endpoint is not called directly. Instead, an update request should be submitted by making a POST to the /parameter-contexts/update-requests endpoint. " +
            "That endpoint will, in turn, call this endpoint.",
        authorizations = {
            @Authorization(value = "Read - /parameter-contexts/{id}"),
            @Authorization(value = "Write - /parameter-contexts/{id}")
        }
    )
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response updateParameterContext(
        @PathParam("id") String contextId,
        @ApiParam(value = "The updated Parameter Context", required = true) ParameterContextEntity requestEntity) {

        // Validate request
        if (requestEntity.getId() == null) {
            throw new IllegalArgumentException("The ID of the Parameter Context must be specified");
        }
        if (!requestEntity.getId().equals(contextId)) {
            throw new IllegalArgumentException("The ID of the Parameter Context must match the ID specified in the URL's path");
        }

        final ParameterContextDTO updateDto = requestEntity.getComponent();
        if (updateDto == null) {
            throw new IllegalArgumentException("The Parameter Context must be supplied");
        }

        final RevisionDTO revisionDto = requestEntity.getRevision();
        if (revisionDto == null) {
            throw new IllegalArgumentException("The Revision of the Parameter Context must be specified.");
        }

        // Perform the request
        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestEntity.isDisconnectedNodeAcknowledged());
        }

        final Revision requestRevision = getRevision(requestEntity.getRevision(), updateDto.getId());
        return withWriteLock(
            serviceFacade,
            requestEntity,
            requestRevision,
            lookup -> {
                // TODO: IMPLEMENT

            },
            () -> {
                // TODO: IMPLEMENT
                serviceFacade.verifyUpdateParameterContext(updateDto, true);
            },
            (rev, entity) -> {
                final ParameterContextEntity updatedEntity = serviceFacade.updateParameterContext(rev, entity.getComponent());

                updatedEntity.setUri(generateResourceUri("parameter-contexts", entity.getId()));
                return generateOkResponse(updatedEntity).build();
            }
        );
    }


    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("update-requests")
    @ApiOperation(
        value = "Initiate the Update Request of a Parameter Context",
        response = ParameterContextUpdateRequestEntity.class,
        notes = "This will initiate the process of updating a Parameter Context. Changing the value of a Parameter may require that one or more components be stopped and " +
            "restarted, so this acttion may take significantly more time than many other REST API actions. As a result, this endpoint will immediately return a ParameterContextUpdateRequestEntity, " +
            "and the process of updating the necessary components will occur asynchronously in the background. The client may then periodically poll the status of the request by " +
            "issuing a GET request to /parameter-contexts/update-requests/{requestId}. Once the request is completed, the client is expected to issue a DELETE request to " +
            "/parameter-contexts/update-requests/{requestId}.",
        authorizations = {
            @Authorization(value = "Read - /process-groups/{uuid}"),
            @Authorization(value = "Write - /process-groups/{uuid}"),
            @Authorization(value = "Read - /parameter-contexts/{parameterContextId}"),
            @Authorization(value = "Write - /parameter-contexts/{parameterContextId}"),
        })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response submitParameterContextUpdate(
        @ApiParam(value = "The updated version of the parameter context.", required = true) final ParameterContextEntity requestEntity) {

        // Verify the request
        final RevisionDTO revisionDto = requestEntity.getRevision();
        if (revisionDto == null) {
            throw new IllegalArgumentException("Parameter Context Revision must be specified");
        }

        final ParameterContextDTO contextDto = requestEntity.getComponent();
        if (contextDto == null) {
            throw new IllegalArgumentException("Parameter Context must be specified");
        }

        if (contextDto.getId() == null) {
            throw new IllegalArgumentException("Parameter Context's ID must be specified");
        }

        // We will perform the updating of the Parameter Context in a background thread because it can be a long-running process.
        // In order to do this, we will need some objects that are only available as Thread-Local variables to the current
        // thread, so we will gather the values for these objects up front.
        final boolean replicateRequest = isReplicateRequest();
        final ComponentLifecycle componentLifecycle = replicateRequest ? clusterComponentLifecycle : localComponentLifecycle;
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // Workflow for this process:
        // 1. Determine which components will be affected and are enabled/running
        // 2. Verify READ and WRITE permissions for user, for every component that is affected
        // 3. Verify READ and WRITE permissions for user, for Parameter Context
        // 4. Stop all Processors that are affected.
        // 5. Wait for all of the Processors to finish stopping.
        // 6. Disable all Controller Services that are affected.
        // 7. Wait for all Controller Services to finish disabling.
        // 8. Update Parameter Context
        // 9. Re-Enable all affected Controller Services
        // 10. Re-Start all Processors

        final Set<AffectedComponentEntity> affectedComponents = serviceFacade.getComponentsAffectedByParameterContextUpdate(contextDto);
        logger.debug("Received Update Request for Parameter Context: {}; the following {} components will be affected: {}", requestEntity, affectedComponents.size(), affectedComponents);

        final InitiateChangeParameterContextRequestWrapper requestWrapper = new InitiateChangeParameterContextRequestWrapper(requestEntity, componentLifecycle, getAbsolutePath(),
            affectedComponents, replicateRequest, user);

        final Revision requestRevision = getRevision(requestEntity.getRevision(), contextDto.getId());
        return withWriteLock(
            serviceFacade,
            requestWrapper,
            requestRevision,
            lookup -> {
                // TODO: Verify READ and WRITE permissions for user, for every component.
                for (final AffectedComponentEntity entity : affectedComponents) {
                    final AffectedComponentDTO dto = entity.getComponent();
                    if (AffectedComponentDTO.COMPONENT_TYPE_PROCESSOR.equals(dto.getReferenceType())) {
                        final ComponentAuthorizable processor = lookup.getProcessor(dto.getId());

                    }
                }

                // TODO: Verify permissions for the Parameter Context
            },
            () -> {
                // Verify Request
                serviceFacade.verifyUpdateParameterContext(contextDto, false);
            },
            this::submitUpdateRequest
        );
    }

    private Response submitUpdateRequest(final Revision requestRevision, final InitiateChangeParameterContextRequestWrapper requestWrapper) {
        // Create an asynchronous request that will occur in the background, because this request may
        // result in stopping components, which can take an indeterminate amount of time.
        final String requestId = UUID.randomUUID().toString();
        final AsynchronousWebRequest<ParameterContextEntity> request = new StandardAsynchronousWebRequest<>(requestId, null, requestWrapper.getUser(), "Stopping Affected Processors");

        // Submit the request to be performed in the background
        final Consumer<AsynchronousWebRequest<ParameterContextEntity>> updateTask = asyncRequest -> {
            try {
                final ParameterContextEntity updatedParameterContextEntity = updateParameterContext(asyncRequest, requestWrapper.getComponentLifecycle(), requestWrapper.getExampleUri(),
                    requestWrapper.getAffectedComponents(), requestWrapper.isReplicateRequest(), requestRevision, requestWrapper.getParameterContextEntity());

                asyncRequest.markComplete(updatedParameterContextEntity);
            } catch (final ResumeFlowException rfe) {
                // Treat ResumeFlowException differently because we don't want to include a message that we couldn't update the flow
                // since in this case the flow was successfully updated - we just couldn't re-enable the components.
                logger.error(rfe.getMessage(), rfe);
                asyncRequest.setFailureReason(rfe.getMessage());
            } catch (final Exception e) {
                logger.error("Failed to update Parameter Context", e);
                asyncRequest.setFailureReason("Failed to update Parameter Context due to " + e);
            }
        };

        requestManager.submitRequest("update-requests", requestId, request, updateTask);

        // Generate the response.
        final ParameterContextUpdateRequestDTO updateRequestDto = new ParameterContextUpdateRequestDTO();
        updateRequestDto.setComplete(request.isComplete());
        updateRequestDto.setFailureReason(request.getFailureReason());
        updateRequestDto.setLastUpdated(request.getLastUpdated());
        updateRequestDto.setRequestId(requestId);
        updateRequestDto.setUri(generateResourceUri("versions", "update-requests", requestId));
        updateRequestDto.setPercentCompleted(request.getPercentComplete());
        updateRequestDto.setState(request.getState());

        final ParameterContextUpdateRequestEntity updateRequestEntity = new ParameterContextUpdateRequestEntity();
        final ParameterContextEntity contextEntity = serviceFacade.getParameterContext(requestWrapper.getParameterContextEntity().getId(), requestWrapper.getUser());
        updateRequestEntity.setParameterContextRevision(contextEntity.getRevision());
        updateRequestEntity.setRequest(updateRequestDto);

        return generateOkResponse(updateRequestEntity).build();
    }


    private ParameterContextEntity updateParameterContext(final AsynchronousWebRequest<ParameterContextEntity> asyncRequest, final ComponentLifecycle componentLifecycle, final URI uri,
                                                          final Set<AffectedComponentEntity> affectedComponents,
                                                          final boolean replicateRequest, final Revision revision, final ParameterContextEntity updatedContextEntity)
        throws LifecycleManagementException, ResumeFlowException {

        final Set<AffectedComponentEntity> runningProcessors = affectedComponents.stream()
            .filter(component -> AffectedComponentDTO.COMPONENT_TYPE_PROCESSOR.equals(component.getComponent().getReferenceType()))
            .filter(component -> "Running".equalsIgnoreCase(component.getComponent().getState()))
            .collect(Collectors.toSet());

        final Set<AffectedComponentEntity> enabledControllerServices = affectedComponents.stream()
            .filter(dto -> AffectedComponentDTO.COMPONENT_TYPE_CONTROLLER_SERVICE.equals(dto.getComponent().getReferenceType()))
            .filter(dto -> "Enabled".equalsIgnoreCase(dto.getComponent().getState()))
            .collect(Collectors.toSet());

        stopProcessors(runningProcessors, asyncRequest, componentLifecycle, uri);
        if (asyncRequest.isCancelled()) {
            return null;
        }

        disableControllerServices(enabledControllerServices, asyncRequest, componentLifecycle, uri);
        if (asyncRequest.isCancelled()) {
            return null;
        }

        asyncRequest.update(new Date(), "Updating ParameterContext", 40);
        logger.info("Updating Parameter Context with ID {}", updatedContextEntity.getId());

        try {
            return performParameterContextUpdate(asyncRequest, uri, replicateRequest, revision, updatedContextEntity);
        } finally {
            // TODO: can almost certainly be refactored so that the same code is shared between VersionsResource and ParameterContextResource.
            if (!asyncRequest.isCancelled()) {
                enableControllerServices(enabledControllerServices, asyncRequest, componentLifecycle, uri);
            }

            if (!asyncRequest.isCancelled()) {
                restartProcessors(runningProcessors, asyncRequest, componentLifecycle, uri);
            }
        }
    }

    private ParameterContextEntity performParameterContextUpdate(final AsynchronousWebRequest<?> asyncRequest, final URI uri, final boolean replicateRequest, final Revision revision,
                                               final ParameterContextEntity updatedContext) {

        if (replicateRequest) {
            // TODO: Implement
            return null;
        } else {
            serviceFacade.verifyUpdateParameterContext(updatedContext.getComponent(), true);
            return serviceFacade.updateParameterContext(revision, updatedContext.getComponent());
        }
    }

    private void stopProcessors(final Set<AffectedComponentEntity> processors, final AsynchronousWebRequest<?> asyncRequest, final ComponentLifecycle componentLifecycle, final URI uri)
        throws LifecycleManagementException {

        logger.info("Stopping {} Processors in order to update Parameter Context", processors.size());
        final CancellableTimedPause stopComponentsPause = new CancellableTimedPause(250, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        asyncRequest.setCancelCallback(stopComponentsPause::cancel);
        componentLifecycle.scheduleComponents(uri, "root", processors, ScheduledState.STOPPED, stopComponentsPause);
    }

    private void restartProcessors(final Set<AffectedComponentEntity> processors, final AsynchronousWebRequest<?> asyncRequest, final ComponentLifecycle componentLifecycle, final URI uri)
        throws ResumeFlowException, LifecycleManagementException {

        if (logger.isDebugEnabled()) {
            logger.debug("Restarting {} Processors after having updated Parameter Context: {}", processors.size(), processors);
        } else {
            logger.info("Restarting {} Processors after having updated Parameter Context", processors.size());
        }

        asyncRequest.update(new Date(), "Restarting Processors", 80);

        // Step 14. Restart all components
        final Set<AffectedComponentEntity> componentsToStart = getUpdatedEntities(processors);

        final CancellableTimedPause startComponentsPause = new CancellableTimedPause(250, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        asyncRequest.setCancelCallback(startComponentsPause::cancel);

        try {
            componentLifecycle.scheduleComponents(uri, "root", componentsToStart, ScheduledState.RUNNING, startComponentsPause);
        } catch (final IllegalStateException ise) {
            // Component Lifecycle will restart the Processors only if they are valid. If IllegalStateException gets thrown, we need to provide
            // a more intelligent error message as to exactly what happened, rather than indicate that the flow could not be updated.
            throw new ResumeFlowException("Failed to restart components because " + ise.getMessage(), ise);
        }
    }

    private void disableControllerServices(final Set<AffectedComponentEntity> controllerServices, final AsynchronousWebRequest<?> asyncRequest, final ComponentLifecycle componentLifecycle,
                                           final URI uri) throws LifecycleManagementException {

        asyncRequest.update(new Date(), "Disabling Affected Controller Services", 20);
        logger.info("Disabling {} Controller Services in order to update Parameter Context", controllerServices.size());
        final CancellableTimedPause disableServicesPause = new CancellableTimedPause(250, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        asyncRequest.setCancelCallback(disableServicesPause::cancel);
        componentLifecycle.activateControllerServices(uri, "root", controllerServices, ControllerServiceState.DISABLED, disableServicesPause);
    }

    private void enableControllerServices(final Set<AffectedComponentEntity> controllerServices, final AsynchronousWebRequest<?> asyncRequest, final ComponentLifecycle componentLifecycle,
                                          final URI uri) throws LifecycleManagementException, ResumeFlowException {
        if (logger.isDebugEnabled()) {
            logger.debug("Re-Enabling {} Controller Services: {}", controllerServices.size(), controllerServices);
        } else {
            logger.info("Re-Enabling {} Controller Services after having updated Parameter Context", controllerServices.size());
        }

        asyncRequest.update(new Date(), "Re-Enabling Controller Services", 60);

        // Step 13. Re-enable all disabled controller services
        final CancellableTimedPause enableServicesPause = new CancellableTimedPause(250, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        asyncRequest.setCancelCallback(enableServicesPause::cancel);
        final Set<AffectedComponentEntity> servicesToEnable = getUpdatedEntities(controllerServices);

        try {
            componentLifecycle.activateControllerServices(uri, "root", servicesToEnable, ControllerServiceState.ENABLED, enableServicesPause);
        } catch (final IllegalStateException ise) {
            // Component Lifecycle will re-enable the Controller Services only if they are valid. If IllegalStateException gets thrown, we need to provide
            // a more intelligent error message as to exactly what happened, rather than indicate that the Parameter Context could not be updated.
            throw new ResumeFlowException("Failed to re-enable Controller Services because " + ise.getMessage(), ise);
        }
    }

    private Set<AffectedComponentEntity> getUpdatedEntities(final Set<AffectedComponentEntity> originalEntities) {
        final Set<AffectedComponentEntity> entities = new LinkedHashSet<>();

        for (final AffectedComponentEntity original : originalEntities) {
            try {
                final AffectedComponentEntity updatedEntity = AffectedComponentUtils.updateEntity(original, serviceFacade, dtoFactory);
                if (updatedEntity != null) {
                    entities.add(updatedEntity);
                }
            } catch (final ResourceNotFoundException rnfe) {
                // Component was removed. Just continue on without adding anything to the entities.
                // We do this because the intent is to get updated versions of the entities with current
                // Revisions so that we can change the states of the components. If the component was removed,
                // then we can just drop the entity, since there is no need to change its state.
            }
        }

        return entities;
    }

    private static class InitiateChangeParameterContextRequestWrapper extends Entity {
        private final ParameterContextEntity parameterContextEntity;
        private final ComponentLifecycle componentLifecycle;
        private final URI exampleUri;
        private final Set<AffectedComponentEntity> affectedComponents;
        private final boolean replicateRequest;
        private final NiFiUser nifiUser;

        public InitiateChangeParameterContextRequestWrapper(final ParameterContextEntity parameterContextEntity, final ComponentLifecycle componentLifecycle,
                                                            final URI exampleUri, final Set<AffectedComponentEntity> affectedComponents, final boolean replicateRequest,
                                                            final NiFiUser nifiUser) {

            this.parameterContextEntity = parameterContextEntity;
            this.componentLifecycle = componentLifecycle;
            this.exampleUri = exampleUri;
            this.affectedComponents = affectedComponents;
            this.replicateRequest = replicateRequest;
            this.nifiUser = nifiUser;
        }

        public ParameterContextEntity getParameterContextEntity() {
            return parameterContextEntity;
        }

        public ComponentLifecycle getComponentLifecycle() {
            return componentLifecycle;
        }

        public URI getExampleUri() {
            return exampleUri;
        }

        public Set<AffectedComponentEntity> getAffectedComponents() {
            return affectedComponents;
        }

        public boolean isReplicateRequest() {
            return replicateRequest;
        }

        public NiFiUser getUser() {
            return nifiUser;
        }
    }


    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setAuthorizer(Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    public void setClusterComponentLifecycle(ComponentLifecycle componentLifecycle) {
        this.clusterComponentLifecycle = componentLifecycle;
    }

    public void setLocalComponentLifecycle(ComponentLifecycle componentLifecycle) {
        this.localComponentLifecycle = componentLifecycle;
    }

    public void setDtoFactory(final DtoFactory dtoFactory) {
        this.dtoFactory = dtoFactory;
    }

}
