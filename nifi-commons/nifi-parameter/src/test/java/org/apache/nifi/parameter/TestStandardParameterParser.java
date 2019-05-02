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

import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestStandardParameterParser {

    @Test
    public void testProperReferences() {
        final ParameterParser parameterParser = new StandardParameterParser();
        final ParameterReferences references = parameterParser.findReferences("#{foo}");

        for (final ParameterReference reference : references) {
            assertEquals("foo", reference.getParameterName().get());
            assertEquals(0, reference.getStartOffset());
            assertEquals(5, reference.getEndOffset());
            assertEquals("#{foo}", reference.getReferenceText());
        }

        List<ParameterReference> referenceList = parameterParser.findReferences("/#{foo}").toReferenceList();
        assertEquals(1, referenceList.size());

        ParameterReference reference = referenceList.get(0);
        assertEquals("foo", reference.getParameterName().get());
        assertEquals(1, reference.getStartOffset());
        assertEquals(6, reference.getEndOffset());
        assertEquals("#{foo}", reference.getReferenceText());

        referenceList = parameterParser.findReferences("/#{foo}/").toReferenceList();
        assertEquals(1, referenceList.size());
        reference = referenceList.get(0);
        assertEquals("foo", reference.getParameterName().get());
        assertEquals(1, reference.getStartOffset());
        assertEquals(6, reference.getEndOffset());
        assertEquals("#{foo}", reference.getReferenceText());

        referenceList = parameterParser.findReferences("/#{foo}/#{bar}#{baz}").toReferenceList();
        assertEquals(3, referenceList.size());

        reference = referenceList.get(0);
        assertEquals("foo", reference.getParameterName().get());
        assertEquals(1, reference.getStartOffset());
        assertEquals(6, reference.getEndOffset());
        assertEquals("#{foo}", reference.getReferenceText());

        reference = referenceList.get(1);
        assertEquals("bar", reference.getParameterName().get());
        assertEquals(8, reference.getStartOffset());
        assertEquals(13, reference.getEndOffset());
        assertEquals("#{bar}", reference.getReferenceText());

        reference = referenceList.get(2);
        assertEquals("baz", reference.getParameterName().get());
        assertEquals(14, reference.getStartOffset());
        assertEquals(19, reference.getEndOffset());
        assertEquals("#{baz}", reference.getReferenceText());
    }

    @Test
    public void testEscapeSequences() {
        final ParameterParser parameterParser = new StandardParameterParser();
        List<ParameterReference> referenceList = parameterParser.findReferences("#{foo}").toReferenceList();
        assertEquals(1, referenceList.size());

        ParameterReference reference = referenceList.get(0);
        assertEquals("foo", reference.getParameterName().get());
        assertEquals(0, reference.getStartOffset());
        assertEquals(5, reference.getEndOffset());
        assertEquals("#{foo}", reference.getReferenceText());
        assertFalse(reference.isEscapeSequence());

        referenceList = parameterParser.findReferences("##{foo}").toReferenceList();
        assertEquals(1, referenceList.size());

        reference = referenceList.get(0);
        assertFalse(reference.getParameterName().isPresent());
        assertEquals(0, reference.getStartOffset());
        assertEquals(6, reference.getEndOffset());
        assertEquals("##{foo}", reference.getReferenceText());
        assertTrue(reference.isEscapeSequence());

        referenceList = parameterParser.findReferences("###{foo}").toReferenceList();
        assertEquals(2, referenceList.size());

        reference = referenceList.get(0);
        assertFalse(reference.getParameterName().isPresent());
        assertEquals(0, reference.getStartOffset());
        assertEquals(1, reference.getEndOffset());
        assertEquals("##", reference.getReferenceText());
        assertTrue(reference.isEscapeSequence());

        reference = referenceList.get(1);
        assertEquals("foo", reference.getParameterName().get());
        assertEquals(2, reference.getStartOffset());
        assertEquals(7, reference.getEndOffset());
        assertFalse(reference.isEscapeSequence());

        // Test an escaped # followed by an escaped #{foo}
        referenceList = parameterParser.findReferences("####{foo}").toReferenceList();
        assertEquals(2, referenceList.size());

        reference = referenceList.get(0);
        assertFalse(reference.getParameterName().isPresent());
        assertEquals(0, reference.getStartOffset());
        assertEquals(1, reference.getEndOffset());
        assertEquals("##", reference.getReferenceText());
        assertTrue(reference.isEscapeSequence());

        reference = referenceList.get(1);
        assertFalse(reference.getParameterName().isPresent());
        assertEquals(2, reference.getStartOffset());
        assertEquals(8, reference.getEndOffset());
        assertTrue(reference.isEscapeSequence());

        // Test multiple escaped # followed by a reference of #{foo}
        referenceList = parameterParser.findReferences("#####{foo}").toReferenceList();
        assertEquals(3, referenceList.size());

        reference = referenceList.get(0);
        assertFalse(reference.getParameterName().isPresent());
        assertEquals(0, reference.getStartOffset());
        assertEquals(1, reference.getEndOffset());
        assertEquals("##", reference.getReferenceText());
        assertTrue(reference.isEscapeSequence());

        reference = referenceList.get(1);
        assertFalse(reference.getParameterName().isPresent());
        assertEquals(2, reference.getStartOffset());
        assertEquals(3, reference.getEndOffset());
        assertEquals("##", reference.getReferenceText());
        assertTrue(reference.isEscapeSequence());

        reference = referenceList.get(2);
        assertEquals("foo", reference.getParameterName().get());
        assertEquals(4, reference.getStartOffset());
        assertEquals(9, reference.getEndOffset());
        assertFalse(reference.isEscapeSequence());
    }

    @Test
    public void testNonReferences() {
        final ParameterParser parameterParser = new StandardParameterParser();

        for (final String input : new String[] {"#foo", "Some text #{blah foo", "#foo}", "#}foo{", "#f{oo}", "#", "##", "###", "####", "#####", "#{", "##{", "###{"}) {
            assertEquals(0, parameterParser.findReferences(input).toReferenceList().size());
        }

    }
}
