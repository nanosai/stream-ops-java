package com.nanosai.streamops.engine.storage.file;

import java.util.ArrayList;
import java.util.List;

public class StreamFileStorage {

    private String rootDirPath = null;

    private List<StreamFileStorageBlock> storageBlocks = new ArrayList<>();

    public StreamFileStorage(String rootDirPath){
        this.rootDirPath = rootDirPath;
    }


}
