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

public interface ParameterReferences extends Iterable<ParameterReference> {

    String substitute(String input, ParameterContext parameterContext);

    List<ParameterReference> toReferenceList();

    ParameterReferences EMPTY = new ParameterReferences() {
        @Override
        public Iterator<ParameterReference> iterator() {
            return Collections.emptyIterator();
        }

        @Override
        public String substitute(final String input, ParameterContext parameterContext) {
            return input;
        }

        @Override
        public List<ParameterReference> toReferenceList() {
            return Collections.emptyList();
        }
    };
}
