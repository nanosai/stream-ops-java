package com.nanosai.streamops.engine.storage.file;

public class StreamFileStorageBlock {

    private   String fileName    = null;
    private   long   firstOffset = 0;
    protected long   fileLength  = 0;


    public StreamFileStorageBlock(String fileName, long fileLength, long firstOffset) {
        this.fileName    = fileName;
        this.fileLength = fileLength;
        this.firstOffset = firstOffset;
    }

    public String getFileName() {
        return this.fileName;
    }

    public long getFirstOffset() {
        return firstOffset;
    }

    public long getFileLength() {
        return this.fileLength;
    }

}
