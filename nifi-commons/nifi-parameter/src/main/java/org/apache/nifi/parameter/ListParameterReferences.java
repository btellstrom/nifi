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
package org.apache.nifi.parameter;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class ListParameterReferences implements ParameterReferences {
    private final List<ParameterReference> references;

    public ListParameterReferences(final List<ParameterReference> references) {
        this.references = references;
    }

    @Override
    public String substitute(final String input, final ParameterContext parameterContext) {
        if (references.isEmpty()) {
            return input;
        }

        final StringBuilder sb = new StringBuilder();

        int lastEndOffset = -1;
        for (final ParameterReference reference : references) {
            final int startOffset = reference.getStartOffset();

            sb.append(input, lastEndOffset + 1, startOffset);
            sb.append(reference.getValue(parameterContext));

            lastEndOffset = reference.getEndOffset();
        }

        if (input.length() > lastEndOffset + 1) {
            sb.append(input, lastEndOffset + 1, input.length());
        }

        return sb.toString();
    }

    @Override
    public List<ParameterReference> toReferenceList() {
        return Collections.unmodifiableList(references);
    }

    @Override
    public Iterator<ParameterReference> iterator() {
        return references.iterator();
    }
}
