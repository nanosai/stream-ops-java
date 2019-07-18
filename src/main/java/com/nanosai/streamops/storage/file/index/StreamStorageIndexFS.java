package com.nanosai.streamops.storage.file.index;

public class StreamStorageIndexFS {

    private final long STREAM_FILE_BYTE_OFFSET_MASK = 0x00000000_FFFFFFFFL;

    private long offsetInterval = 1024;

    private long[] records    = null;
    private int    usedLength = 0;

    public StreamStorageIndexFS(int indexLength, long offsetInterval) {
        this.records        = new long[indexLength];
        this.offsetInterval = offsetInterval;
    }


    public void append(long streamRecordOffset, int streamFileStorageBlockIndex, long streamFileByteOffset) {
        if (streamRecordOffset % this.offsetInterval == 0) {
            long indexRecord = streamFileStorageBlockIndex;
            indexRecord <<= 32;
            indexRecord |= STREAM_FILE_BYTE_OFFSET_MASK & streamFileByteOffset;
            this.records[this.usedLength++] = indexRecord;
        }
    }


    public long lookup(long streamRecordOffset){
        int closestRecordIndex = (int) (streamRecordOffset / this.offsetInterval);

        if(closestRecordIndex >= usedLength){
            return -1;
        }

        return this.records[closestRecordIndex];
    }
}
