package com.nanosai.streamops.engine.storage.file;

public class StreamFileStorageBlock {

    private String filePath    = null;
    private long   firstOffset = 0;
    public  long   size        = 0;


    public StreamFileStorageBlock(String filePath, long size, long firstOffset) {
        this.filePath = filePath;
        this.size = size;
        this.firstOffset = firstOffset;
    }

    public String getFilePath() {
        return filePath;
    }

    public long getFirstOffset() {
        return firstOffset;
    }


}
