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
package org.apache.nifi.web.api.dto;

import io.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlType;
import java.util.Date;

@XmlType(name = "parameterContextUpdateRequest")
public class ParameterContextUpdateRequestDTO {
    private String requestId;
    private String uri;
    private Date lastUpdated;
    private boolean complete = false;
    private String failureReason;
    private int percentCompleted;
    private String state;
    private ParameterContextDTO parameterContext;

    @ApiModelProperty(value = "The ID of the request", readOnly = true)
    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(final String requestId) {
        this.requestId = requestId;
    }

    @ApiModelProperty(value = "The URI for the request", readOnly = true)
    public String getUri() {
        return uri;
    }

    public void setUri(final String uri) {
        this.uri = uri;
    }

    @ApiModelProperty(value = "The timestamp of when the request was last updated", readOnly = true)
    public Date getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(final Date lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    @ApiModelProperty(value = "Whether or not the request is completed", readOnly = true)
    public boolean isComplete() {
        return complete;
    }

    public void setComplete(final boolean complete) {
        this.complete = complete;
    }

    @ApiModelProperty(value = "The reason for the request failing, or null if the request has not failed", readOnly = true)
    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(final String failureReason) {
        this.failureReason = failureReason;
    }

    @ApiModelProperty(value = "A value between 0 and 100 (inclusive) indicating how close the request is to completion", readOnly = true)
    public int getPercentCompleted() {
        return percentCompleted;
    }

    public void setPercentCompleted(final int percentCompleted) {
        this.percentCompleted = percentCompleted;
    }

    @ApiModelProperty(value = "A description of the current state of the request", readOnly = true)
    public String getState() {
        return state;
    }

    public void setState(final String state) {
        this.state = state;
    }

    @ApiModelProperty(value = "The Parameter Context that is being operated on. This may not be populated until the request has successfully completed.", readOnly = true)
    public ParameterContextDTO getParameterContext() {
        return parameterContext;
    }

    public void setParameterContext(final ParameterContextDTO parameterContext) {
        this.parameterContext = parameterContext;
    }
}
