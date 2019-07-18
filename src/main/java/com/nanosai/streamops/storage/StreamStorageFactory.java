package com.nanosai.streamops.storage;

import com.nanosai.streamops.storage.file.StreamStorageFS;
import com.nanosai.streamops.storage.file.StreamStorageRootFS;

import java.io.IOException;

public class StreamStorageFactory {


    public StreamStorageRootFS createStreamStorageRootFs(String rootDirPath){
        return new StreamStorageRootFS(rootDirPath);
    }

    public StreamStorageFS createStreamStorageFS(String streamId, String rootDirPath) throws IOException {
        return new StreamStorageFS(streamId, rootDirPath);
    }

    public StreamStorageFS createStreamStorageFS(String streamId, String rootDirPath, long maxFileBlockSize) throws IOException {
        return new StreamStorageFS(streamId, rootDirPath, maxFileBlockSize);
    }

}
