package com.nanosai.streamops.engine.storage.file;

import java.io.File;
import java.io.IOException;

public class StreamStorageRootFS {

    private String rootDirPath = null;

    public StreamStorageRootFS(String rootDirPath){
        this.rootDirPath = rootDirPath;
        new File(this.rootDirPath).mkdirs();
    }


    public String getRootDirPath() {
        return this.rootDirPath;
    }


    public StreamStorageFS createStreamStorage(String streamId) throws IOException {
        String streamFileStorageRootDir = createStreamFileStorageRootDirPath(streamId);
        new File(streamFileStorageRootDir).mkdirs();

        return new StreamStorageFS(streamId, streamFileStorageRootDir);
    }

    public StreamStorageFS createStreamStorage(String streamId, long storageFileBlockFileMaxSize) throws IOException {
        String streamFileStorageRootDir = createStreamFileStorageRootDirPath(streamId);
        new File(streamFileStorageRootDir).mkdirs();

        return new StreamStorageFS(streamId, streamFileStorageRootDir, storageFileBlockFileMaxSize);
    }

    private String createStreamFileStorageRootDirPath(String streamId) {
        return this.rootDirPath + "/" + streamId;
    }


}
