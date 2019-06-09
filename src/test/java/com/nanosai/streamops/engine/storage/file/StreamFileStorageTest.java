package com.nanosai.streamops.engine.storage.file;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class StreamFileStorageTest {

    @Test
    public void testCreateNewStreamFileStorage() throws IOException {
        StreamFileStorage streamFileStorage = new StreamFileStorage("test-stream-1", "data/test-stream-1");

        assertNotNull(streamFileStorage.getLatestBlock());
        assertEquals("test-stream-1-0000000000000000", streamFileStorage.getLatestBlock().getFileName());
        assertEquals(1, streamFileStorage.getStorageBlocks().size());
    }

    @Test
    public void testAppendToStreamFileStorage() throws IOException {
        initStreamDir("data/test-stream-1");

        StreamFileStorage streamFileStorage = new StreamFileStorage("test-stream-1", "data/test-stream-1");

        byte[] rionBytesRecord = new byte[]{0x01, 0x08, 0,1,2,3,4,5,6,7}; //10 bytes in total, 8 bytes in the RION Bytes field body

        //todo next step is to test append, and see that files are created and data written to it.
        streamFileStorage.openForAppend();
        streamFileStorage.appendRecord(rionBytesRecord, 0, rionBytesRecord.length);
        streamFileStorage.closeForAppend();

        assertEquals(13, streamFileStorage.getLatestBlock().fileLength);

        streamFileStorage.openForAppend();
        streamFileStorage.appendRecord(rionBytesRecord, 0, rionBytesRecord.length);
        streamFileStorage.closeForAppend();

        assertEquals(23, streamFileStorage.getLatestBlock().fileLength);

    }




    @Test
    public void testAppendSplitsIntoMultipleFiles() throws IOException {
        initStreamDir("data/test-stream-1");

        StreamFileStorage streamFileStorage = new StreamFileStorage("test-stream-1", "data/test-stream-1", 20);

        byte[] rionBytesRecord1 = new byte[]{0x01, 0x08, 0,1,2,3,4,5,6,7}; //10 bytes in total, 8 bytes in the RION Bytes field body
        byte[] rionBytesRecord2 = new byte[]{0x01, 0x08, 0,1,2,3,4,5,6,7}; //10 bytes in total, 8 bytes in the RION Bytes field body
        byte[] rionBytesRecord3 = new byte[]{0x01, 0x04, 0,1,2,3};         //10 bytes in total, 8 bytes in the RION Bytes field body
        byte[] rionBytesRecord4 = new byte[]{0x01, 0x04, 0,2,4,6};        //10 bytes in total, 8 bytes in the RION Bytes field body

        streamFileStorage.openForAppend();
        streamFileStorage.appendRecord(rionBytesRecord1, 0, rionBytesRecord1.length);
        streamFileStorage.appendRecord(rionBytesRecord2, 0, rionBytesRecord2.length);
        streamFileStorage.appendRecord(rionBytesRecord3, 0, rionBytesRecord3.length);
        streamFileStorage.appendRecord(rionBytesRecord4, 0, rionBytesRecord4.length);
        streamFileStorage.closeForAppend();

        assertEquals(3, streamFileStorage.getStorageBlocks().size());
        assertEquals(13, streamFileStorage.getStorageBlocks().get(0).fileLength);
        assertEquals(19, streamFileStorage.getStorageBlocks().get(1).fileLength);
        assertEquals( 9, streamFileStorage.getStorageBlocks().get(2).fileLength);

        byte[] dest1 = new byte[13];

        int bytesRead1 = streamFileStorage.readStreamFileStorageBlock(
                streamFileStorage.getStorageBlocks().get(0), 0, dest1, 0, 13);

        assertEquals(13, bytesRead1);

        assertEquals(0xF1, 255 & dest1[0]); // first  byte of a RION Extended Field with field byte length 1
        assertEquals(0x01, 255 & dest1[1]); // second byte of a RION Extended Field with field type 1 ( Extended 1 = stream record offset).
        assertEquals(0x00, 255 & dest1[2]); // record offset is expected to be 0.

        assertEquals(0x01, 255 & dest1[3]); // lead byte of RION Bytes field - with length byte count 1
        assertEquals(0x08, 255 & dest1[4]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes

        assertEquals(0x00, 255 & dest1[5]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x01, 255 & dest1[6]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x02, 255 & dest1[7]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x03, 255 & dest1[8]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x04, 255 & dest1[9]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x05, 255 & dest1[10]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x06, 255 & dest1[11]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x07, 255 & dest1[12]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes

        byte[] dest2 = new byte[19];

        int bytesRead2 = streamFileStorage.readStreamFileStorageBlock(
                streamFileStorage.getStorageBlocks().get(1), 0, dest2, 0, 19);

        assertEquals(19, bytesRead2);

        assertEquals(0xF1, 255 & dest2[0]); // first  byte of a RION Extended Field with field byte length 1
        assertEquals(0x01, 255 & dest2[1]); // second byte of a RION Extended Field with field type 1 ( Extended 1 = stream record offset).
        assertEquals(0x01, 255 & dest2[2]); // record offset is expected to be 1.

        assertEquals(0x01, 255 & dest2[3]); // lead byte of RION Bytes field - with length byte count 1
        assertEquals(0x08, 255 & dest2[4]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes

        assertEquals(0x00, 255 & dest2[5]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x01, 255 & dest2[6]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x02, 255 & dest2[7]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x03, 255 & dest2[8]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x04, 255 & dest2[9]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x05, 255 & dest2[10]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x06, 255 & dest2[11]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x07, 255 & dest2[12]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes

        assertEquals(0x01, 255 & dest2[13]); // lead byte of RION Bytes field - with length byte count 1
        assertEquals(0x04, 255 & dest2[14]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes

        assertEquals(0x00, 255 & dest2[15]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x01, 255 & dest2[16]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x02, 255 & dest2[17]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x03, 255 & dest2[18]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes

        byte[] dest3 = new byte[9];

        int bytesRead3 = streamFileStorage.readStreamFileStorageBlock(
                streamFileStorage.getStorageBlocks().get(2), 0, dest3, 0, 9);

        assertEquals(9, bytesRead3);

        assertEquals(0xF1, 255 & dest3[0]); // first  byte of a RION Extended Field with field byte length 1
        assertEquals(0x01, 255 & dest3[1]); // second byte of a RION Extended Field with field type 1 ( Extended 1 = stream record offset).
        assertEquals(0x03, 255 & dest3[2]); // record offset is expected to be 1.

        assertEquals(0x01, 255 & dest3[3]); // lead byte of RION Bytes field - with length byte count 1
        assertEquals(0x04, 255 & dest3[4]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes

        assertEquals(0x00, 255 & dest3[5]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x02, 255 & dest3[6]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x04, 255 & dest3[7]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x06, 255 & dest3[8]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes

    }


    private void initStreamDir(String streamRootDirPath) {
        deleteDir(streamRootDirPath);
        new File(streamRootDirPath).mkdirs();
    }

    private void deleteDir(String dirPath) {
        File streamRootDir = new File(dirPath);
        if(streamRootDir.exists()) {
            for(File file : streamRootDir.listFiles()){
                file.delete();
            }
            streamRootDir.delete();
        }
    }

    @Test
    public void testToHex() {
        assertEquals("0000000000000000", StreamFileStorage.toHex(0x0));
        assertEquals("000000000000000F", StreamFileStorage.toHex(0xF));
        assertEquals("00000000000000FF", StreamFileStorage.toHex(0xFF));
        assertEquals("FEDCBA9876543210", StreamFileStorage.toHex(0xFEDCBA9876543210L));
    }
}
