package com.nanosai.streamops.examples;

import com.nanosai.streamops.StreamOps;
import com.nanosai.streamops.storage.StreamStorageFactory;
import com.nanosai.streamops.storage.file.StreamStorageRootFS;

import java.io.IOException;

public class StreamOpsExamples {

    public static void main(String[] args) throws IOException {
        StreamStorageFactory streamStorageFactory = StreamOps.createStreamStorageFactory();

        StreamStorageRootFS streamStorageRootFs =
                streamStorageFactory.createStreamStorageRootFs("data/test-root2");

        streamStorageRootFs.createStreamStorage("test-stream-1");



    }
}
