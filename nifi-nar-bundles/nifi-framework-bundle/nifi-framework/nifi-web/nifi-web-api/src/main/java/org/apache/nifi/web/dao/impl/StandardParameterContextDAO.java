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
package org.apache.nifi.web.dao.impl;

import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterDTO;
import org.apache.nifi.web.dao.ParameterContextDAO;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

public class StandardParameterContextDAO implements ParameterContextDAO {
    private FlowManager flowManager;

    @Override
    public boolean hasParameterContext(final String parameterContextId) {
        return flowManager.getParameterContextManager().getParameterContext(parameterContextId) != null;
    }

    @Override
    public void verifyCreate(final ParameterContextDTO parameterContextDto) {
        verifyNoNamingConflict(parameterContextDto.getName());
    }

    @Override
    public ParameterContext createParameterContext(final ParameterContextDTO parameterContextDto) {
        final Set<Parameter> parameters = getParameters(parameterContextDto);
        return flowManager.createParameterContext(parameterContextDto.getId(), parameterContextDto.getName(), parameters);
    }

    private Set<Parameter> getParameters(final ParameterContextDTO parameterContextDto) {
        final Set<ParameterDTO> parameterDtos = parameterContextDto.getParameters();
        if (parameterDtos == null) {
            return Collections.emptySet();
        }

        return parameterContextDto.getParameters().stream()
            .map(this::createParameter)
            .collect(Collectors.toSet());

    }

    private Parameter createParameter(final ParameterDTO dto) {
        final ParameterDescriptor descriptor = new ParameterDescriptor.Builder()
            .name(dto.getName())
            .description(dto.getDescription())
            .sensitive(Boolean.TRUE.equals(dto.getSensitive()))
            .build();

        return new Parameter(descriptor, dto.getValue());
    }

    @Override
    public ParameterContext getParameterContext(final String parameterContextId) {
        final ParameterContext context = flowManager.getParameterContextManager().getParameterContext(parameterContextId);
        if (context == null) {
            throw new ResourceNotFoundException(String.format("Unable to find Parameter Context with id '%s'.", parameterContextId));
        }

        return context;
    }

    @Override
    public Set<ParameterContext> getParameterContexts() {
        return flowManager.getParameterContextManager().getParameterContexts();
    }

    @Override
    public ParameterContext updateParameterContext(final ParameterContextDTO parameterContextDto) {
        verifyUpdate(parameterContextDto, true);

        final ParameterContext context = getParameterContext(parameterContextDto.getId());

        if (parameterContextDto.getName() != null) {
            final boolean conflict = flowManager.getParameterContextManager().getParameterContexts().stream()
                .anyMatch(paramContext -> paramContext.getName().equals(parameterContextDto.getName()));

            if (conflict) {
                throw new IllegalStateException("Cannot update Paramter Context name because another Parameter Context already exists with the name '" + parameterContextDto.getName() + "'");
            }

            context.setName(parameterContextDto.getName());
        }

        if (parameterContextDto.getParameters() != null) {
            final Set<Parameter> parameters = getParameters(parameterContextDto);
            context.setParameters(parameters);
        }

        return context;
    }

    @Override
    public void verifyUpdate(final ParameterContextDTO parameterContextDto, final boolean verifyComponentStates) {
        verifyNoNamingConflict(parameterContextDto.getName());

        // TODO: Need to verify that the updates are valid/legal
        // TODO: In some cases, need to verify that no active components are referencing the Parameter Context.
    }

    private void verifyNoNamingConflict(final String contextName) {
        if (contextName == null) {
            return;
        }

        final boolean conflict = flowManager.getParameterContextManager().getParameterContexts().stream()
            .anyMatch(paramContext -> paramContext.getName().equals(contextName));

        if (conflict) {
            throw new IllegalStateException("Cannot update Paramter Context name because another Parameter Context already exists with the name '" + contextName + "'");
        }
    }

    @Override
    public void verifyDelete(final String parameterContextId) {
        // TODO: Need to verify that Parameter Context not in use by any 'active' components
        final ParameterContext context = getParameterContext(parameterContextId);

    }

    @Override
    public void deleteParameterContext(final String parameterContextId) {
        verifyDelete(parameterContextId);

        // TODO: Implement
    }

    public void setFlowController(final FlowController flowController) {
        this.flowManager = flowController.getFlowManager();
    }
}
