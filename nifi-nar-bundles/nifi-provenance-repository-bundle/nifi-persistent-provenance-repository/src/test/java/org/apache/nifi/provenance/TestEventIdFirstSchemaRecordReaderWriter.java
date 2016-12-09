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

package org.apache.nifi.provenance;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.provenance.serialization.RecordReader;
import org.apache.nifi.provenance.serialization.RecordWriter;
import org.apache.nifi.provenance.toc.StandardTocReader;
import org.apache.nifi.provenance.toc.StandardTocWriter;
import org.apache.nifi.provenance.toc.TocReader;
import org.apache.nifi.provenance.toc.TocUtil;
import org.apache.nifi.provenance.toc.TocWriter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class TestEventIdFirstSchemaRecordReaderWriter extends AbstractTestRecordReaderWriter {
    private final AtomicLong idGenerator = new AtomicLong(0L);
    private File journalFile;
    private File tocFile;

    @BeforeClass
    public static void setupLogger() {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi", "DEBUG");
    }

    @Before
    public void setup() {
        journalFile = new File("target/storage/" + UUID.randomUUID().toString() + "/testEventIdFirstSchemaRecordReaderWriter");
        tocFile = TocUtil.getTocFile(journalFile);
        idGenerator.set(0L);
    };

    @Override
    protected RecordWriter createWriter(final File file, final TocWriter tocWriter, final boolean compressed, final int uncompressedBlockSize) throws IOException {
        return new EventIdFirstSchemaRecordWriter(file, idGenerator, tocWriter, compressed, uncompressedBlockSize);
    }

    @Override
    protected RecordReader createReader(final InputStream in, final String journalFilename, final TocReader tocReader, final int maxAttributeSize) throws IOException {
        return new EventIdFirstSchemaRecordReader(in, journalFilename, tocReader, maxAttributeSize);
    }

    @Test
    @Ignore
    public void testPerformanceOfRandomAccessReads() throws Exception {
        journalFile = new File("target/storage/" + UUID.randomUUID().toString() + "/testPerformanceOfRandomAccessReads.gz");
        tocFile = TocUtil.getTocFile(journalFile);

        final int blockSize = 1024 * 32;
        try (final RecordWriter writer = createWriter(journalFile, new StandardTocWriter(tocFile, true, false), true, blockSize)) {
            writer.writeHeader(0L);

            for (int i = 0; i < 100_000; i++) {
                writer.writeRecord(createEvent());
            }
        }

        final long[] eventIds = new long[] {
            4, 80, 1024, 1025, 1026, 1027, 1028, 1029, 1030, 40_000, 80_000, 99_000
        };

        boolean loopForever = true;
        while (loopForever) {
            final long start = System.nanoTime();
            for (int i = 0; i < 1000; i++) {
                try (final InputStream in = new FileInputStream(journalFile);
                    final RecordReader reader = createReader(in, journalFile.getName(), new StandardTocReader(tocFile), 32 * 1024)) {

                    for (final long id : eventIds) {
                        time(() -> {
                            reader.skipToEvent(id);
                            return reader.nextRecord();
                        }, id);
                    }
                }
            }

            final long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            System.out.println(ms + " ms total");
        }
    }

    private void time(final Callable<StandardProvenanceEventRecord> task, final long id) throws Exception {
        final long start = System.nanoTime();
        final StandardProvenanceEventRecord event = task.call();
        Assert.assertNotNull(event);
        Assert.assertEquals(id, event.getEventId());
        //        System.out.println(event);
        final long nanos = System.nanoTime() - start;
        final long millis = TimeUnit.NANOSECONDS.toMillis(nanos);
        //        System.out.println("Took " + millis + " ms to " + taskDescription);
    }
}