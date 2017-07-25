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

import java.util.HashSet;
import java.util.Set;

import javax.xml.bind.annotation.XmlType;

import com.wordnik.swagger.annotations.ApiModelProperty;

@XmlType(name = "variable")
public class VariableDTO {
    private String name;
    private String value;
    private Set<AffectedComponentDTO> affectedComponents = new HashSet<>();
    private boolean canUpdate;

    @ApiModelProperty("The name of the variable")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @ApiModelProperty("The value of the variable")
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @ApiModelProperty("A set of all components that will be affected if the value of this variable is changed")
    public Set<AffectedComponentDTO> getAffectedComponents() {
        return affectedComponents;
    }

    public void setAffectedComponents(Set<AffectedComponentDTO> affectedComponents) {
        this.affectedComponents = affectedComponents;
    }

    @ApiModelProperty("Whether or not the user making the request has appropriate permissions to update this variable")
    public boolean getCanUpdate() {
        return canUpdate;
    }

    public void setCanUpdate(boolean canUpdate) {
        this.canUpdate = canUpdate;
    }
}
