package org.apache.nifi.controller.queue.clustered.client;

import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.flowfile.FlowFile;

import java.io.IOException;
import java.io.OutputStream;

public interface LoadBalanceFlowFileCodec {
    void encode(FlowFileRecord flowFile, OutputStream out) throws IOException;
}
