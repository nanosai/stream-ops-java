package com.nanosai.streamops.engine.storage.file.index;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StreamStorageIndexFSTest {


    @Test
    public void test() {
        int offsetInterval = 1024;
        StreamStorageIndexFS index = new StreamStorageIndexFS(1024 * 1024, offsetInterval);

        int  recordsPerFile  = 4096;
        long recordOffsetMax = 1024L * 1024L;

        index.append(800, 0, 1600);
        index.append(800, 0, 1600);

        for(long recordOffset = 0; recordOffset < recordOffsetMax; recordOffset++){
            int streamFileStorageBlockIndex = (int) (recordOffset / recordsPerFile);  //doesn't make sense for real records, but fine for testing
            int streamFileByteOffset        = (int) (recordOffset % recordsPerFile);  //doesn't make sense for real records, but fine for testing

            index.append(recordOffset, streamFileStorageBlockIndex, streamFileByteOffset);
        }

        for(long recordOffset = 0; recordOffset < recordOffsetMax; recordOffset++){
            long indexRecord = index.lookup(recordOffset);

            int streamFileStorageBlockIndex = (int) (indexRecord >> 32);
            int streamFileByteOffset        = (int) (0x0000_0000_FFFF_FFFFL & indexRecord);

            int expectedStreamFileStorageBlockIndex = (int) (recordOffset / recordsPerFile);
            assertEquals(expectedStreamFileStorageBlockIndex, streamFileStorageBlockIndex);

            int expectedStreamFileByteOffset = (int) (((recordOffset / offsetInterval) * offsetInterval) % recordsPerFile);

            assertEquals(expectedStreamFileByteOffset, streamFileByteOffset);
        }


        //sample tests to make sure I have not made logical mistakes in the loops above

        long recordOffset = recordsPerFile + offsetInterval + 1;
        long indexRecord = index.lookup(recordOffset);

        int streamFileStorageBlockIndex = (int) (indexRecord >> 32);
        assertEquals(1, streamFileStorageBlockIndex);

        int streamFileByteOffset = (int) (0x0000_0000_FFFF_FFFF & indexRecord);
        assertEquals(offsetInterval, streamFileByteOffset);
    }
}
