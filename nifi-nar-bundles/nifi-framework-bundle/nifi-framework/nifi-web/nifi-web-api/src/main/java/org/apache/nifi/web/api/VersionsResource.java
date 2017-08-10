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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.ComponentAuthorizable;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.AffectedComponentDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.dto.VersionedFlowDTO;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.VersionControlComponentMappingEntity;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.apache.nifi.web.api.entity.VersionedFlowEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.apache.nifi.web.util.CancellableTimedPause;
import org.apache.nifi.web.util.ComponentLifecycle;
import org.apache.nifi.web.util.LifecycleManagementException;
import org.apache.nifi.web.util.Pause;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.core.ResourceContext;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import com.wordnik.swagger.annotations.Authorization;

@Path("/versions")
@Api(value = "/versions", description = "Endpoint for managing version control for a flow")
public class VersionsResource extends ApplicationResource {
    private static final Logger logger = LoggerFactory.getLogger(VersionsResource.class);

    @Context
    private ResourceContext resourceContext;
    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;
    private ComponentLifecycle componentLifecycle;

    private ActiveRequest activeRequest = null;
    private final Object activeRequestMonitor = new Object();


    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("process-groups/{id}")
    @ApiOperation(value = "Gets the Version Control information for a process group",
        response = VersionControlInformationEntity.class,
        authorizations = {
            @Authorization(value = "Read - /process-groups/{uuid}", type = "")
        })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response getVersionInformation(@ApiParam(value = "The process group id.", required = true) @PathParam("id") final String groupId) {
        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable processGroup = lookup.getProcessGroup(groupId).getAuthorizable();
            processGroup.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the version control information for this process group
        final VersionControlInformationEntity entity = serviceFacade.getVersionControlInformation(groupId);
        if (entity == null) {
            throw new ResourceNotFoundException("Process Group with ID " + groupId + " is not currently under Version Control");
        }

        return generateOkResponse(entity).build();
    }


    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("start-requests")
    @ApiOperation(value = "Creates a request so that a Process Group can be placed under Version Control or have its Version Control configuration changed",
        response = VersionControlInformationEntity.class)
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response createVersionControlRequest() throws InterruptedException {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST);
        }

        return withWriteLock(
            serviceFacade,
            /* entity */ null,
            lookup -> {},
            /* verifier */ null,
            requestEntity -> {
                final String requestId = generateUuid();

                // We need to ensure that only a single Version Control Request can occur throughout the flow.
                // Otherwise, User 1 could log into Node 1 and choose to Version Control Group A.
                // At the same time, User 2 could log into Node 2 and choose to Version Control Group B, which is a child of Group A.
                // As a result, may could end up in a situation where we are creating flows in the registry that are never referenced.
                synchronized (activeRequestMonitor) {
                    if (activeRequest == null || activeRequest.isExpired()) {
                        activeRequest = new ActiveRequest(requestId);
                    } else {
                        throw new IllegalStateException("A request is already underway to place a Process Group in this NiFi instance under Version Control. "
                            + "Only a single such request is allowed to occurred at a time. Please try the request again momentarily.");
                    }
                }

                return generateOkResponse(requestId).build();
            });
    }


    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("start-requests/{id}")
    @ApiOperation(value = "Updates the request with the given ID",
        response = VersionControlInformationEntity.class,
        authorizations = {
            @Authorization(value = "Write - /process-groups/{uuid}", type = "")
        })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response updateVersionControlRequest(@ApiParam("The request ID.") @PathParam("id") final String requestId,
        @ApiParam(value = "The controller service configuration details.", required = true) final VersionControlComponentMappingEntity requestEntity) {

        // Verify request
        final RevisionDTO revisionDto = requestEntity.getProcessGroupRevision();
        if (revisionDto == null) {
            throw new IllegalArgumentException("Process Group Revision must be specified");
        }

        final VersionControlInformationDTO versionControlInfo = requestEntity.getVersionControlInformation();
        if (versionControlInfo == null) {
            throw new IllegalArgumentException("Version Control Information must be supplied");
        }
        if (versionControlInfo.getGroupId() == null) {
            throw new IllegalArgumentException("Version Control Information must supply Process Group ID");
        }
        if (versionControlInfo.getBucketId() == null) {
            throw new IllegalArgumentException("Version Control Information must supply Bucket ID");
        }
        if (versionControlInfo.getFlowId() == null) {
            throw new IllegalArgumentException("Version Control Information must supply Flow ID");
        }
        if (versionControlInfo.getRegistryId() == null) {
            throw new IllegalArgumentException("Version Control Information must supply Registry ID");
        }
        if (versionControlInfo.getVersion() == null) {
            throw new IllegalArgumentException("Version Control Information must supply Version");
        }

        final Map<String, String> mapping = requestEntity.getVersionControlComponentMapping();
        if (mapping == null) {
            throw new IllegalArgumentException("Version Control Component Mapping must be supplied");
        }

        // Replicate if necessary
        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestEntity);
        }

        // Perform the update
        synchronized (activeRequestMonitor) {
            if (activeRequest == null) {
                throw new IllegalStateException("No Version Control Request with ID " + requestId + " is currently active");
            }

            if (!requestId.equals(activeRequest.getRequestId())) {
                throw new IllegalStateException("No Version Control Request with ID " + requestId + " is currently active");
            }

            if (activeRequest.isExpired()) {
                throw new IllegalStateException("Version Control Request with ID " + requestId + " has already expired");
            }

            final String groupId = requestEntity.getVersionControlInformation().getGroupId();

            final Revision groupRevision = new Revision(revisionDto.getVersion(), revisionDto.getClientId(), groupId);
            return withWriteLock(
                serviceFacade,
                requestEntity,
                groupRevision,
                lookup -> {
                    final Authorizable processGroup = lookup.getProcessGroup(groupId).getAuthorizable();
                    processGroup.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                (rev, mappingEntity) -> {
                    final VersionControlInformationEntity responseEntity = serviceFacade.setVersionControlInformation(rev, groupId,
                        mappingEntity.getVersionControlInformation(), mappingEntity.getVersionControlComponentMapping());
                    return generateOkResponse(responseEntity).build();
                });
        }
    }


    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("start-requests/{id}")
    @ApiOperation(value = "Deletes the request with the given ID")
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response deleteVersionControlRequest(@ApiParam("The request ID.") @PathParam("id") final String requestId) {
        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        }

        return withWriteLock(
            serviceFacade,
            null,
            lookup -> {},
            null,
            requestEntity -> {
                synchronized (activeRequestMonitor) {
                    if (activeRequest == null) {
                        throw new IllegalStateException("No Version Control Request with ID " + requestId + " is currently active");
                    }

                    if (!requestId.equals(activeRequest.getRequestId())) {
                        throw new IllegalStateException("No Version Control Request with ID " + requestId + " is currently active");
                    }

                    activeRequest = null;
                }

                return generateOkResponse().build();
            });

    }


    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("process-groups/{id}")
    @ApiOperation(value = "Begins version controlling the Process Group with the given ID",
        response = VersionControlInformationEntity.class,
        authorizations = {
            @Authorization(value = "Read - /process-groups/{uuid}", type = "")
        })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response startVersionControl(
        @ApiParam("The process group id.") @PathParam("id") final String groupId,
        @ApiParam(value = "The controller service configuration details.", required = true) final VersionedFlowEntity requestEntity) throws IOException {

        // Verify the request
        final RevisionDTO revisionDto = requestEntity.getProcessGroupRevision();
        if (revisionDto == null) {
            throw new IllegalArgumentException("Process Group Revision must be specified");
        }

        final VersionedFlowDTO versionedFlowDto = requestEntity.getVersionedFlow();
        if (versionedFlowDto == null) {
            throw new IllegalArgumentException("Version Control Information must be supplied.");
        }
        if (versionedFlowDto.getBucketId() == null) {
            throw new IllegalArgumentException("The Bucket ID must be supplied.");
        }
        if (versionedFlowDto.getFlowName() == null) {
            throw new IllegalArgumentException("The Flow Name must be supplied.");
        }
        if (versionedFlowDto.getRegistryId() == null) {
            throw new IllegalArgumentException("The Registry ID must be supplied.");
        }

        if (isReplicateRequest()) {
            // We first have to obtain a "lock" on all nodes in the cluster so that multiple Version Control requests
            // are not being made simultaneously. We do this by making a POST to /nifi-api/versions/requests.
            // The Response gives us back the Request ID.
            final Client jerseyClient = createJerseyClient();
            try {
                final URI requestUri;
                try {
                    final URI originalUri = getAbsolutePath();
                    final URI createRequestUri = new URI(originalUri.getScheme(), originalUri.getUserInfo(), originalUri.getHost(),
                        originalUri.getPort(), "/nifi-api/versions/start-requests", null, originalUri.getFragment());

                    final ClientResponse response = jerseyClient.resource(createRequestUri)
                        .type(MediaType.APPLICATION_JSON)
                        .post(ClientResponse.class);

                    if (response.getStatus() != Status.OK.getStatusCode()) {
                        final String errorResponse = response.getEntity(String.class);
                        throw new IllegalStateException(
                            "Failed to create a Version Control Request across all nodes in the cluster. Received response code " + response.getStatus() + " with content: " + errorResponse);
                    }

                    final String requestId = response.getEntity(String.class);

                    requestUri = new URI(originalUri.getScheme(), originalUri.getUserInfo(), originalUri.getHost(),
                        originalUri.getPort(), "/nifi-api/versions/start-requests/" + requestId, null, originalUri.getFragment());
                } catch (final URISyntaxException e) {
                    throw new RuntimeException(e);
                }


                // Now that we have the Request, we know that no other thread is updating the Flow Registry. So we can now
                // create the Flow in the Flow Registry and push the Process Group as the first version of the Flow. Once we've
                // succeeded with that, we need to update all nodes' Process Group to contain the new Version Control Information.
                // Finally, we can delete the Request.
                try {
                    final VersionControlComponentMappingEntity mappingEntity = serviceFacade.registerFlowWithFlowRegistry(groupId, requestEntity);

                    // TODO: Fix how we replicate! In many places!
                    final ClientResponse updateVersionControlInfoRepsonse = jerseyClient.resource(requestUri)
                        .type(MediaType.APPLICATION_JSON)
                        .entity(mappingEntity)
                        .put(ClientResponse.class);

                    if (updateVersionControlInfoRepsonse.getStatus() != Status.OK.getStatusCode()) {
                        throw new IllegalStateException("Failed to update Version Control Information for Process Group with ID " + groupId + ".");
                    }

                    final VersionControlInformationEntity responseEntity = serviceFacade.getVersionControlInformation(groupId);
                    return generateOkResponse(responseEntity).build();
                } finally {
                    final ClientResponse deleteResponse = jerseyClient
                        .resource(requestUri)
                        .type(MediaType.APPLICATION_JSON)
                        .delete(ClientResponse.class);

                    if (deleteResponse.getStatus() != Status.OK.getStatusCode()) {
                        logger.error("After starting Version Control on Process Group with ID " + groupId + ", failed to delete Version Control Request. "
                            + "Users may be unable to Version Control other Process Groups until the request lock times out. Response status code was " + deleteResponse.getStatus());
                    }
                }
            } finally {
                jerseyClient.destroy();
            }
        }

        // Perform local task. If running in a cluster environment, we will never get to this point. This is because
        // in the above block, we check if (isReplicate()) and if true, we implement the 'cluster logic', but this
        // does not involve replicating the actual request, because we only want a single node to handle the logic of
        // creating the flow in the Registry.
        final Revision groupRevision = new Revision(revisionDto.getVersion(), revisionDto.getClientId(), groupId);
        return withWriteLock(
            serviceFacade,
            requestEntity,
            groupRevision,
            lookup -> {
                final Authorizable processGroup = lookup.getProcessGroup(groupId).getAuthorizable();
                processGroup.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
                processGroup.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
            },
            () -> {
                final VersionControlInformationEntity entity = serviceFacade.getVersionControlInformation(groupId);
                if (entity != null) {
                    throw new IllegalStateException("Cannot place Process Group with ID " + groupId + " under Version Control because it already is under Version Control");
                }
            },
            (rev, flowEntity) -> {
                // Register the current flow with the Flow Registry.
                final VersionControlComponentMappingEntity mappingEntity = serviceFacade.registerFlowWithFlowRegistry(groupId, requestEntity);

                // Update the Process Group's Version Control Information
                final VersionControlInformationEntity responseEntity = serviceFacade.setVersionControlInformation(rev, groupId,
                    mappingEntity.getVersionControlInformation(), mappingEntity.getVersionControlComponentMapping());
                return generateOkResponse(responseEntity).build();
            });
    }


    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("process-groups/{id}")
    @ApiOperation(value = "Stops version controlling the Process Group with the given ID",
        response = VersionControlInformationEntity.class,
        authorizations = {
            @Authorization(value = "Read - /process-groups/{uuid}", type = ""),
            @Authorization(value = "Write - /process-groups/{uuid}", type = ""),
        })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response stopVersionControl(
        @ApiParam(value = "The version is used to verify the client is working with the latest version of the flow.", required = false) @QueryParam(VERSION) final LongParameter version,

        @ApiParam(value = "If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.", required = false)
        @QueryParam(CLIENT_ID)
        @DefaultValue(StringUtils.EMPTY) final ClientIdParameter clientId,

        @ApiParam("The process group id.") @PathParam("id") final String groupId) throws IOException {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        }

        final Revision requestRevision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), groupId);
        return withWriteLock(
            serviceFacade,
            null,
            requestRevision,
            lookup -> {
                final Authorizable processGroup = lookup.getProcessGroup(groupId).getAuthorizable();
                processGroup.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
                processGroup.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
            },
            () -> {
                final VersionControlInformationEntity currentVersionControlInfo = serviceFacade.getVersionControlInformation(groupId);
                if (currentVersionControlInfo == null) {
                    throw new IllegalStateException("Process Group with ID " + groupId + " is not currently under Version Control");
                }
            },
            (revision, groupEntity) -> {
                // set the version control info to null
                final VersionControlInformationEntity entity = serviceFacade.setVersionControlInformation(requestRevision, groupId, null, null);

                // generate the response
                return generateOkResponse(entity).build();
            });
    }



    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("update-requests/process-groups/{id}")
    @ApiOperation(value = "For a Process Group that is already under Version Control, this will initiate the action of changing "
        + "from a specific version of the flow in the Flow Registry to a different version of the flow.",
        response = VersionControlInformationEntity.class,
        authorizations = {
            @Authorization(value = "Read - /process-groups/{uuid}", type = ""),
            @Authorization(value = "Write - /process-groups/{uuid}", type = "")
        })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response initiateVersionControlUpdate(
        @ApiParam("The process group id.") @PathParam("id") final String groupId,
        @ApiParam(value = "The controller service configuration details.", required = true) final VersionControlInformationEntity requestEntity) throws IOException, LifecycleManagementException {

        // Verify the request
        final RevisionDTO revisionDto = requestEntity.getProcessGroupRevision();
        if (revisionDto == null) {
            throw new IllegalArgumentException("Process Group Revision must be specified");
        }

        final VersionControlInformationDTO versionControlInfoDto = requestEntity.getVersionControlInformation();
        if (versionControlInfoDto == null) {
            throw new IllegalArgumentException("Version Control Information must be supplied.");
        }
        if (versionControlInfoDto.getGroupId() == null) {
            throw new IllegalArgumentException("The Process Group ID must be supplied.");
        }
        if (!versionControlInfoDto.getGroupId().equals(groupId)) {
            throw new IllegalArgumentException("The Process Group ID in the request body does not match the Process Group ID of the requested resource.");
        }
        if (versionControlInfoDto.getBucketId() == null) {
            throw new IllegalArgumentException("The Bucket ID must be supplied.");
        }
        if (versionControlInfoDto.getFlowId() == null) {
            throw new IllegalArgumentException("The Flow ID must be supplied.");
        }
        if (versionControlInfoDto.getRegistryId() == null) {
            throw new IllegalArgumentException("The Registry ID must be supplied.");
        }
        if (versionControlInfoDto.getVersion() == null) {
            throw new IllegalArgumentException("The Version of the flow must be supplied.");
        }


        // TODO: Do in background
        // TODO: Cluster-ize


        // Workflow for this process:
        // 0. Obtain the versioned flow snapshot to use for the update
        //    a. Contact registry to download the desired version.
        //    b. Get Variable Registry of this Process Group and all ancestor groups
        //    c. Perform diff to find any new variables
        //    d. Get Variable Registry of any child Process Group in the versioned flow
        //    e. Perform diff to find any new variables
        //    f. Prompt user to fill in values for all new variables
        // 1. Verify that all components in the snapshot exist on all nodes (i.e., the NAR exists)?
        // 2. Verify that Process Group is already under version control. If not, must start Version Control instead of updateFlow
        // 3. Verify that Process Group is not 'dirty'.
        // 4. Determine which components would be affected (and are enabled/running)
        //    a. Component itself is modified in some way, other than position changing.
        //    b. Source and Destination of any Connection that is modified.
        //    c. Any Processor or Controller Service that references a Controller Service that is modified.
        // 5. Verify READ and WRITE permissions for user, for every component affected.
        // 6. Stop all Processors, Funnels, Ports that are affected.
        // 7. Wait for all of the components to finish stopping.
        // 8. Disable all Controller Services that are affected.
        // 9. Wait for all Controller Services to finish disabling.
        // 10. Ensure that if any connection was deleted, that it has no data in it. Ensure that no Input Port
        //    was removed, unless it currently has no incoming connections. Ensure that no Output Port was removed,
        //    unless it currently has no outgoing connections. Checking ports & connections could be done before
        //    stopping everything, but removal of Connections cannot.
        // 11. Update variable registry to include new variables
        //    (only new variables so don't have to worry about affected components? Or do we need to in case a processor
        //    is already referencing the variable? In which case we need to include the affected components above in the
        //    Set of affected components before stopping/disabling.).
        // 12. Update components in the Process Group, update Version Control Information.
        // 13. Re-Enable all affected Controller Services that were not removed.
        // 14. Re-Start all Processors, Funnels, Ports that are affected and not removed.

        // Step 0: Get the Versioned FLow Snapshot from the Flow Registry
        // TODO: Haven't taken into account the Variable Registry yet!
        final VersionedFlowSnapshot flowSnapshot = serviceFacade.getVersionedFlowSnapshot(requestEntity.getVersionControlInformation());

        // Step 1: Verify that all components in the snapshot exist on all nodes
        // TODO:

        // Step 2: Verify that Process Group is already under version control. If not, must start Version Control instead of updating flow
        final VersionControlInformationEntity currentVersionControlInfo = serviceFacade.getVersionControlInformation(groupId);
        if (currentVersionControlInfo == null) {
            throw new IllegalStateException("Cannot change the Version of the flow for Process Group with ID " + groupId
                + " because the Process Group is not currently under Version Control");
        }

        // Step 3: Verify that Process Group is not 'dirty'
        final VersionControlInformationDTO currentVersionDto = currentVersionControlInfo.getVersionControlInformation();
        final Boolean modified = currentVersionDto.getModified();
        if (modified == null) {
            throw new IllegalStateException("Cannot change the Version of the flow for Process Group with ID " + groupId
                + " because the Process Group may have been modified since it was last synchronized with the Flow Registry. The Process Group must be"
                + " synched with the Flow Registry before continuing. This will happen periodically in the background, so please try the request again"
                + " later");
        }
        if (!Boolean.FALSE.equals(modified)) {
            throw new IllegalStateException("Cannot change the Version of the flow for Process Group with ID " + groupId
                + " because the Process Group has been modified since it was last synchronized with the Flow Registry. The Process Group must be"
                + " restored to its original form before changing the version");
        }

        // Step 4: Determine which components will be affected by updating the version
        final Set<AffectedComponentEntity> affectedComponents = serviceFacade.getComponentsAffectedByVersionChange(groupId, flowSnapshot);

        // Step 5: Verify read/write permissions for user for all components affected
        serviceFacade.authorizeAccess(lookup -> {
            final Map<String, List<AffectedComponentEntity>> componentsByType = affectedComponents.stream()
                .collect(Collectors.groupingBy(entity -> entity.getComponent().getReferenceType()));

            // TODO: Add the rest of the types. Input Ports, Output Ports, Funnels, Labels, etc.
            authorize(componentsByType.get(AffectedComponentDTO.COMPONENT_TYPE_PROCESSOR), id -> lookup.getProcessor(id));
            authorize(componentsByType.get(AffectedComponentDTO.COMPONENT_TYPE_CONTROLLER_SERVICE), id -> lookup.getControllerService(id));
        });

        // Step 6: Determine which components must be stopped and stop them.
        // 6a. Determine which components must be stopped
        final Set<String> stoppableComponentTypes = new HashSet<>();
        // TODO: add the rest of the types. Input Ports, Output Ports, Funnels, RPGs, etc.
        stoppableComponentTypes.add(AffectedComponentDTO.COMPONENT_TYPE_PROCESSOR);

        final Set<AffectedComponentEntity> runningComponents = affectedComponents.stream()
            .filter(dto -> stoppableComponentTypes.contains(dto.getComponent().getReferenceType()))
            .filter(dto -> "Running".equalsIgnoreCase(dto.getComponent().getState()))
            .collect(Collectors.toSet());

        final Set<AffectedComponentEntity> enabledServices = affectedComponents.stream()
            .filter(dto -> AffectedComponentDTO.COMPONENT_TYPE_CONTROLLER_SERVICE.equals(dto.getComponent().getReferenceType()))
            .filter(dto -> "Enabled".equalsIgnoreCase(dto.getComponent().getState()))
            .collect(Collectors.toSet());

        // Steps 6-7. Stop running components
        final Pause stopComponentsPause = new CancellableTimedPause(250, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        componentLifecycle.scheduleComponents(getAbsolutePath(), groupId, runningComponents, ScheduledState.STOPPED, stopComponentsPause);

        // Steps 8-9. Disable enabled controller services
        final Pause disableServicesPause = new CancellableTimedPause(250, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        componentLifecycle.activateControllerServices(getAbsolutePath(), groupId, enabledServices, ControllerServiceState.DISABLED, disableServicesPause);

        // Step 10: Ensure that if any connection was deleted, that it has no data in it. Ensure that no Input Port
        //    was removed, unless it currently has no incoming connections. Ensure that no Output Port was removed,
        //    unless it currently has no outgoing connections.
        serviceFacade.verifyCanUpdate(groupId, flowSnapshot);

        // Step 11. Update variable registry to include new variables
        //    (only new variables so don't have to worry about affected components? Or do we need to in case a processor
        //    is already referencing the variable? In which case we need to include the affected components above in the
        //    Set of affected components before stopping/disabling.).
        // TODO: ^^^^

        // Step 12. Update Process Group to the new flow
        final Revision revision = new Revision(revisionDto.getVersion(), revisionDto.getClientId(), groupId);
        final VersionControlInformationDTO vci = requestEntity.getVersionControlInformation();
        serviceFacade.updateProcessGroup(revision, groupId, vci, flowSnapshot, getIdGenerationSeed().orElse(null));

        // Step 13. Re-enable all disabled controller services
        final Pause enableServicesPause = new CancellableTimedPause(250, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        final Set<AffectedComponentEntity> servicesToEnable = getUpdatedEntities(enabledServices);
        componentLifecycle.activateControllerServices(getAbsolutePath(), groupId, servicesToEnable, ControllerServiceState.ENABLED, enableServicesPause);

        // Step 14. Restart all components
        final Set<AffectedComponentEntity> componentsToStart = getUpdatedEntities(runningComponents);
        final Pause startComponentsPause = new CancellableTimedPause(250, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        componentLifecycle.scheduleComponents(getAbsolutePath(), groupId, componentsToStart, ScheduledState.RUNNING, startComponentsPause);

        // TODO: Add logging for each step, such as STARTING COMPONENTS, DISABLING SERVICES, etc.
        final VersionControlInformationEntity responseEntity = serviceFacade.getVersionControlInformation(groupId);
        return generateOkResponse(responseEntity).build();
    }

    private Set<AffectedComponentEntity> getUpdatedEntities(final Set<AffectedComponentEntity> originalEntities) {
        final Set<AffectedComponentEntity> entities = new LinkedHashSet<>();

        for (final AffectedComponentEntity original : originalEntities) {
            final AffectedComponentDTO dto = original.getComponent();

            try {
                // TODO: Take into account other types of components
                switch (dto.getReferenceType()) {
                    case AffectedComponentDTO.COMPONENT_TYPE_CONTROLLER_SERVICE: {
                        final ControllerServiceEntity serviceEntity = serviceFacade.getControllerService(dto.getId());
                        final AffectedComponentEntity affectedEntity = new AffectedComponentEntity();
                        affectedEntity.setBulletins(serviceEntity.getBulletins());
                        affectedEntity.setId(serviceEntity.getId());
                        affectedEntity.setPermissions(serviceEntity.getPermissions());
                        affectedEntity.setPosition(serviceEntity.getPosition());
                        affectedEntity.setRevision(serviceEntity.getRevision());
                        affectedEntity.setUri(serviceEntity.getUri());

                        final AffectedComponentDTO component = new AffectedComponentDTO();
                        component.setId(serviceEntity.getId());
                        component.setReferenceType(AffectedComponentDTO.COMPONENT_TYPE_CONTROLLER_SERVICE);
                        component.setProcessGroupId(serviceEntity.getComponent().getParentGroupId());
                        component.setState(serviceEntity.getComponent().getState());
                        affectedEntity.setComponent(component);

                        entities.add(affectedEntity);
                        break;
                    }
                    case AffectedComponentDTO.COMPONENT_TYPE_PROCESSOR: {
                        final ProcessorEntity procEntity = serviceFacade.getProcessor(dto.getId());
                        final AffectedComponentEntity affectedEntity = new AffectedComponentEntity();
                        affectedEntity.setBulletins(procEntity.getBulletins());
                        affectedEntity.setId(procEntity.getId());
                        affectedEntity.setPermissions(procEntity.getPermissions());
                        affectedEntity.setPosition(procEntity.getPosition());
                        affectedEntity.setRevision(procEntity.getRevision());
                        affectedEntity.setUri(procEntity.getUri());

                        final AffectedComponentDTO component = new AffectedComponentDTO();
                        component.setId(procEntity.getId());
                        component.setReferenceType(AffectedComponentDTO.COMPONENT_TYPE_PROCESSOR);
                        component.setProcessGroupId(procEntity.getComponent().getParentGroupId());
                        component.setState(procEntity.getComponent().getState());
                        affectedEntity.setComponent(component);

                        entities.add(affectedEntity);
                        break;
                    }
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


    private void authorize(final List<AffectedComponentEntity> componentDtos, final Function<String, ComponentAuthorizable> authFunction) {
        if (componentDtos != null) {
            for (final AffectedComponentEntity entity : componentDtos) {
                final Authorizable authorizable = authFunction.apply(entity.getComponent().getId()).getAuthorizable();
                authorizable.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
                authorizable.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
            }
        }
    }



    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setAuthorizer(Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    public void setComponentLifecycle(ComponentLifecycle componentLifecycle) {
        this.componentLifecycle = componentLifecycle;
    }


    private static class ActiveRequest {
        private static final long MAX_REQUEST_LOCK_NANOS = TimeUnit.MINUTES.toNanos(1L);

        private final String requestId;
        private final long creationNanos = System.nanoTime();
        private final long expirationTime = creationNanos + MAX_REQUEST_LOCK_NANOS;

        private ActiveRequest(final String requestId) {
            this.requestId = requestId;
        }

        public boolean isExpired() {
            return System.nanoTime() > expirationTime;
        }

        public String getRequestId() {
            return requestId;
        }
    }
}
