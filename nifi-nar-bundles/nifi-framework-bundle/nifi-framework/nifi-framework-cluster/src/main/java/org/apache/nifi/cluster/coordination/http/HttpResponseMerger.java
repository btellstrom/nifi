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

package org.apache.nifi.cluster.coordination.http;

import java.net.URI;
import java.util.Set;

import org.apache.nifi.cluster.manager.NodeResponse;

/**
 * <p>
 * An HttpResponseMapper is responsible for taking the responses from all nodes in a cluster
 * and distilling them down to a single response that would be appropriate to respond with, to the
 * user/client who made the original web requests.
 * </p>
 */
public interface HttpResponseMerger {

    /**
     * Maps the responses from all nodes in the cluster to a single NodeResponse object that
     * is appropriate to respond with
     *
     * @param uri the URI of the web request that was made
     * @param httpMethod the HTTP Method that was used when making the request
     * @param nodeResponses the responses received from the individual nodes
     *
     * @return a single NodeResponse that represents the response that should be returned to the user/client
     */
    NodeResponse mergeResponses(URI uri, String httpMethod, Set<NodeResponse> nodeResponses);

    /**
     * Returns a subset (or equal set) of the given Node Responses, such that all of those returned are the responses
     * that indicate that the node was unable to fulfill the request
     *
     * @param allResponses the responses to filter
     *
     * @return a subset (or equal set) of the given Node Responses, such that all of those returned are the responses
     *         that indicate that the node was unable to fulfill the request
     */
    Set<NodeResponse> getProblematicNodeResponses(Set<NodeResponse> allResponses);
}
