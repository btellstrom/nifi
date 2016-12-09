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

package org.apache.nifi.provenance.serialization;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.provenance.toc.StandardTocReader;
import org.apache.nifi.provenance.toc.StandardTocWriter;
import org.apache.nifi.provenance.toc.TocReader;
import org.apache.nifi.provenance.toc.TocUtil;
import org.apache.nifi.provenance.toc.TocWriter;
import org.apache.nifi.provenance.util.CloseableUtil;
import org.apache.nifi.stream.io.ByteCountingOutputStream;
import org.apache.nifi.stream.io.GZIPOutputStream;
import org.apache.nifi.stream.io.NonCloseableOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventFileCompressor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(EventFileCompressor.class);
    private final BlockingQueue<File> filesToCompress;
    private volatile boolean shutdown = false;

    public EventFileCompressor(final BlockingQueue<File> filesToCompress) {
        this.filesToCompress = filesToCompress;
    }

    public void shutdown() {
        shutdown = true;
    }

    @Override
    public void run() {
        while (!shutdown) {
            File file = null;

            try {
                final long start = System.nanoTime();
                file = filesToCompress.poll(1, TimeUnit.SECONDS);
                if (file == null || shutdown) {
                    continue;
                }

                StandardTocReader tocReader = null;
                StandardTocWriter tocWriter = null;
                File outputFile = null;

                final File tocFile = TocUtil.getTocFile(file);
                try {
                    tocReader = new StandardTocReader(tocFile);
                } catch (final IOException e) {
                    logger.error("Failed to read TOC File {}", tocFile, e);
                    continue;
                }

                final long bytesBefore = file.length();

                try {
                    outputFile = new File(file.getParentFile(), file.getName() + ".gz");
                    try {
                        final File tmpFile = new File(tocFile.getParentFile(), tocFile.getName() + ".tmp");
                        tocWriter = new StandardTocWriter(tmpFile, true, false);
                        compress(file, tocReader, outputFile, tocWriter);
                        tocWriter.close();
                        tmpFile.renameTo(tocFile);
                    } catch (final IOException ioe) {
                        logger.error("Failed to compress {} on rollover", file, ioe);
                    }
                } finally {
                    CloseableUtil.closeQuietly(tocReader, tocWriter);
                }

                final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                final long bytesAfter = outputFile.length();
                final double reduction = 100 * (1 - (double) bytesAfter / (double) bytesBefore);
                final String reductionTwoDecimals = String.format("%.2f", reduction);
                logger.debug("Successfully compressed Provenance Event File {} in {} millis from {} to {}, a reduction of {}%",
                    file, millis, FormatUtils.formatDataSize(bytesBefore), FormatUtils.formatDataSize(bytesAfter), reductionTwoDecimals);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (final Exception e) {
                logger.error("Failed to compress {}", file, e);
            }
        }
    }

    public static void compress(final File input, final TocReader tocReader, final File output, final TocWriter tocWriter) throws IOException {
        try (final InputStream fis = new FileInputStream(input);
            final OutputStream fos = new FileOutputStream(output);
            final ByteCountingOutputStream byteCountingOut = new ByteCountingOutputStream(fos)) {

            int blockIndex = 0;
            while (true) {
                // Determine the min and max byte ranges for the current block.
                final long blockStart = tocReader.getBlockOffset(blockIndex);
                if (blockStart == -1) {
                    break;
                }

                long blockEnd = tocReader.getBlockOffset(blockIndex + 1);
                if (blockEnd < 0) {
                    blockEnd = input.length();
                }

                final long firstEventId = tocReader.getFirstEventIdForBlock(blockIndex);
                final long blockStartOffset = byteCountingOut.getBytesWritten();

                try (final OutputStream ncos = new NonCloseableOutputStream(byteCountingOut);
                    final OutputStream gzipOut = new GZIPOutputStream(ncos, 1)) {
                    StreamUtils.copy(fis, gzipOut, blockEnd - blockStart);
                }

                tocWriter.addBlockOffset(blockStartOffset, firstEventId);
                blockIndex++;
            }
        }

        // Close the TOC Reader and TOC Writer
        CloseableUtil.closeQuietly(tocReader, tocWriter);

        // Attempt to delete the input file and associated toc file
        if (input.delete()) {
            final File tocFile = tocReader.getFile();
            if (!tocFile.delete()) {
                logger.warn("Failed to delete {}; this file should be cleaned up manually", tocFile);
            }
        } else {
            logger.warn("Failed to delete {}; this file should be cleaned up manually", input);
        }
    }
}