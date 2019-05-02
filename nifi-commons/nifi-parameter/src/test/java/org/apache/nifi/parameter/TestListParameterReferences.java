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

import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestListParameterReferences {

    @Test
    public void testSubstitute() {
        final List<ParameterReference> referenceList = new ArrayList<>();
        referenceList.add(new StandardParameterReference("foo", 0, 5, "#{foo}"));

        final ParameterContext paramContext = Mockito.mock(ParameterContext.class);
        Mockito.when(paramContext.getParameter("foo")).thenReturn(new Parameter(new ParameterDescriptor.Builder().name("foo").build(), "bar"));
        Mockito.when(paramContext.getParameter("bazz")).thenReturn(new Parameter(new ParameterDescriptor.Builder().name("bazz").build(), "baz"));

        final ListParameterReferences references = new ListParameterReferences(referenceList);
        assertEquals("bar", references.substitute("#{foo}", paramContext));

        referenceList.add(new StandardParameterReference("bazz", 6, 12, "#{bazz}"));
        assertEquals("barbaz", references.substitute("#{foo}#{bazz}", paramContext));

        assertEquals("barbazHello, World!", references.substitute("#{foo}#{bazz}Hello, World!", paramContext));

        referenceList.clear();
        referenceList.add(new StandardParameterReference("foo", 0, 5, "#{foo}"));
    }

    @Test
    public void testSubstituteWithReferenceToNonExistentParameter() {
        final List<ParameterReference> referenceList = new ArrayList<>();
        referenceList.add(new StandardParameterReference("foo", 0, 5, "#{foo}"));

        final ParameterContext paramContext = Mockito.mock(ParameterContext.class);
        final ListParameterReferences references = new ListParameterReferences(referenceList);

        assertEquals("#{foo}", references.substitute("#{foo}", paramContext));
    }

    @Test
    public void testSubstituteWithEscapes() {
        final List<ParameterReference> referenceList = new ArrayList<>();
        referenceList.add(new StartCharacterEscape(0));
        referenceList.add(new EscapedParameterReference(2, 8, "##{foo}"));

        final ParameterContext paramContext = Mockito.mock(ParameterContext.class);
        Mockito.when(paramContext.getParameter("foo")).thenReturn(new Parameter(new ParameterDescriptor.Builder().name("foo").build(), "bar"));

        final ListParameterReferences references = new ListParameterReferences(referenceList);
        assertEquals("##{foo}", references.substitute("####{foo}", paramContext));

        referenceList.add(new StandardParameterReference("foo", 12, 17, "#{foo}"));
        assertEquals("##{foo}***bar", references.substitute("####{foo}***#{foo}", paramContext));

    }
}
