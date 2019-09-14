package com.nanosai.streamops.storage.file;

import com.nanosai.streamops.util.FileUtil;
import com.nanosai.streamops.util.HexUtil;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

public class StreamStorageFSTest {

    @Test
    public void testCreateNewStreamFileStorage() throws IOException {
        FileUtil.resetDir(new File("data/test-stream-1"));
        StreamStorageFS streamStorageFS = new StreamStorageFS("test-stream-1", "data/test-stream-1");

        assertNotNull(streamStorageFS.getLatestBlock());
        assertEquals("test-stream-1-0000000000000000", streamStorageFS.getLatestBlock().getFileName());
        assertEquals(1, streamStorageFS.getStorageBlocks().size());
    }

    @Test
    public void testAppendToStreamFileStorage() throws IOException {
        FileUtil.resetDir(new File("data/test-stream-1"));
        initStreamDir("data/test-stream-1");

        StreamStorageFS streamStorageFS = new StreamStorageFS("test-stream-1", "data/test-stream-1");

        byte[] rionBytesRecord = new byte[]{0x01, 0x08, 0,1,2,3,4,5,6,7}; //10 bytes in total, 8 bytes in the RION Bytes field body

        //todo next step is to test append, and see that files are created and data written to it.
        streamStorageFS.openForAppend();
        streamStorageFS.appendRecord(rionBytesRecord, 0, rionBytesRecord.length);
        streamStorageFS.closeForAppend();

        assertEquals(14, streamStorageFS.getLatestBlock().fileLength);

        streamStorageFS.openForAppend();
        streamStorageFS.appendRecord(rionBytesRecord, 0, rionBytesRecord.length);
        streamStorageFS.closeForAppend();

        assertEquals(24, streamStorageFS.getLatestBlock().fileLength);

    }

    @Test
    public void testAppendListenerCalled() throws IOException {
        FileUtil.resetDir(new File("data/test-stream-1"));

        StreamStorageFS streamStorageFS = new StreamStorageFS("test-stream-1", "data/test-stream-1", 20);

        byte[] rionBytesRecord1 = new byte[]{0x01, 0x08, 0,1,2,3,4,5,6,7}; //10 bytes in total, 8 bytes in the RION Bytes field body

        AtomicBoolean appendListenerCalled = new AtomicBoolean();
        appendListenerCalled.set(false);
        streamStorageFS.setAppendListener((byteArray, offset, length, recordOffset) -> {
            appendListenerCalled.set(true);
        });
        streamStorageFS.openForAppend();
        streamStorageFS.appendRecord(rionBytesRecord1, 0, rionBytesRecord1.length);

        assertTrue(appendListenerCalled.get());

        streamStorageFS.closeForAppend();

    }


    @Test
    public void testAppendSplitsIntoMultipleFiles() throws IOException {
        FileUtil.resetDir(new File("data/test-stream-1"));
        //initStreamDir("data/test-stream-1");

        StreamStorageFS streamStorageFS = new StreamStorageFS("test-stream-1", "data/test-stream-1", 20);

        byte[] rionBytesRecord1 = new byte[]{0x01, 0x08, 0,1,2,3,4,5,6,7}; //10 bytes in total, 8 bytes in the RION Bytes field body
        byte[] rionBytesRecord2 = new byte[]{0x01, 0x08, 0,1,2,3,4,5,6,7}; //10 bytes in total, 8 bytes in the RION Bytes field body
        byte[] rionBytesRecord3 = new byte[]{0x01, 0x04, 0,1,2,3};         //10 bytes in total, 8 bytes in the RION Bytes field body
        byte[] rionBytesRecord4 = new byte[]{0x01, 0x04, 0,2,4,6};        //10 bytes in total, 8 bytes in the RION Bytes field body

        streamStorageFS.openForAppend();
        streamStorageFS.appendRecord(rionBytesRecord1, 0, rionBytesRecord1.length);
        streamStorageFS.appendRecord(rionBytesRecord2, 0, rionBytesRecord2.length);
        streamStorageFS.appendRecord(rionBytesRecord3, 0, rionBytesRecord3.length);
        streamStorageFS.appendRecord(rionBytesRecord4, 0, rionBytesRecord4.length);
        streamStorageFS.closeForAppend();

        assertEquals(3, streamStorageFS.getStorageBlocks().size());
        assertEquals(14, streamStorageFS.getStorageBlocks().get(0).fileLength);
        assertEquals(20, streamStorageFS.getStorageBlocks().get(1).fileLength);
        assertEquals(10, streamStorageFS.getStorageBlocks().get(2).fileLength);

        byte[] dest1 = new byte[14];

        int bytesRead1 = streamStorageFS.readFromBlock(
                streamStorageFS.getStorageBlocks().get(0), 0, dest1, 0, 14);

        assertEquals(14, bytesRead1);

        assertEquals(0xF1, 255 & dest1[0]); // first  byte of a RION Extended Field with field byte length 1
        assertEquals(0x01, 255 & dest1[1]); // second byte of a RION Extended Field with field type 1 ( Extended 1 = stream record offset).
        assertEquals(   1, 255 & dest1[2]); // length byte of a RION Extended Field - expected to be 1 byte.
        assertEquals(0x00, 255 & dest1[3]); // record offset is expected to be 0.

        assertEquals(0x01, 255 & dest1[4]); // lead byte of RION Bytes field - with length byte count 1
        assertEquals(0x08, 255 & dest1[5]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes

        assertEquals(0x00, 255 & dest1[6]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x01, 255 & dest1[7]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x02, 255 & dest1[8]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x03, 255 & dest1[9]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x04, 255 & dest1[10]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x05, 255 & dest1[11]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x06, 255 & dest1[12]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x07, 255 & dest1[13]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes

        byte[] dest2 = new byte[20];

        int bytesRead2 = streamStorageFS.readFromBlock(
                streamStorageFS.getStorageBlocks().get(1), 0, dest2, 0, 20);

        assertEquals(20, bytesRead2);

        assertEquals(0xF1, 255 & dest2[0]); // first  byte of a RION Extended Field with field byte length 1
        assertEquals(0x01, 255 & dest2[1]); // second byte of a RION Extended Field with field type 1 ( Extended 1 = stream record offset).
        assertEquals(0x01, 255 & dest2[2]); // third byte of a RION Extended Field - length byte with value 1.
        assertEquals(0x01, 255 & dest2[3]); // record offset is expected to be 1.

        assertEquals(0x01, 255 & dest2[4]); // lead byte of RION Bytes field - with length byte count 1
        assertEquals(0x08, 255 & dest2[5]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes

        assertEquals(0x00, 255 & dest2[6]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x01, 255 & dest2[7]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x02, 255 & dest2[8]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x03, 255 & dest2[9]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x04, 255 & dest2[10]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x05, 255 & dest2[11]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x06, 255 & dest2[12]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x07, 255 & dest2[13]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes

        assertEquals(0x01, 255 & dest2[14]); // lead byte of RION Bytes field - with length byte count 1
        assertEquals(0x04, 255 & dest2[15]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes

        assertEquals(0x00, 255 & dest2[16]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x01, 255 & dest2[17]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x02, 255 & dest2[18]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x03, 255 & dest2[19]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes

        byte[] dest3 = new byte[10];

        int bytesRead3 = streamStorageFS.readFromBlock(
                streamStorageFS.getStorageBlocks().get(2), 0, dest3, 0, 10);

        assertEquals(10, bytesRead3);

        assertEquals(0xF1, 255 & dest3[0]); // first  byte of a RION Extended Field with field byte length 1
        assertEquals(0x01, 255 & dest3[1]); // second byte of a RION Extended Field with field type 1 ( Extended 1 = stream record offset).
        assertEquals(   1, 255 & dest3[2]); // third byte of a RION Extended Field -length byte with a value of 1.
        assertEquals(0x03, 255 & dest3[3]); // record offset is expected to be 1.

        assertEquals(0x01, 255 & dest3[4]); // lead byte of RION Bytes field - with length byte count 1
        assertEquals(0x04, 255 & dest3[5]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes

        assertEquals(0x00, 255 & dest3[6]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x02, 255 & dest3[7]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x04, 255 & dest3[8]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x06, 255 & dest3[9]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes

    }


    @Test
    public void testIterateFrom() throws IOException {
        FileUtil.resetDir(new File("data/test-stream-1"));
        StreamStorageFS streamStorageFS = new StreamStorageFS("test-stream-1", "data/test-stream-1", 34);

        byte[] rionBytesRecord1 = new byte[]{0x01, 0x08, 0,1,2,3,4,5,6,7}; //10 bytes in total, 8 bytes in the RION Bytes field body

        streamStorageFS.openForAppend();
        streamStorageFS.appendRecord(rionBytesRecord1, 0, rionBytesRecord1.length);
        streamStorageFS.appendRecord(rionBytesRecord1, 0, rionBytesRecord1.length);
        streamStorageFS.appendRecord(rionBytesRecord1, 0, rionBytesRecord1.length);
        streamStorageFS.appendRecord(rionBytesRecord1, 0, rionBytesRecord1.length);
        streamStorageFS.appendRecord(rionBytesRecord1, 0, rionBytesRecord1.length);
        streamStorageFS.appendRecord(rionBytesRecord1, 0, rionBytesRecord1.length);
        streamStorageFS.closeForAppend();

        assertEquals(2, streamStorageFS.getStorageBlocks().size());


        byte[] recordBuffer = new byte[34];
        AtomicLong firstOffset = new AtomicLong();
        AtomicLong numberOfIterations = new AtomicLong();

        streamStorageFS.iterateFromOffset(recordBuffer, 4, (offset, rionReader) -> {
            firstOffset.set(offset);
            numberOfIterations.incrementAndGet();
            return false;
        });

        assertEquals(4, firstOffset.get());
        assertEquals(1, numberOfIterations.get());

    }


    private void initStreamDir(String streamRootDirPath) {
        deleteDir(streamRootDirPath);
        new File(streamRootDirPath).mkdirs();
    }

    private void deleteDir(String dirPath) {
        File streamRootDir = new File(dirPath);
        if(streamRootDir.exists()) {
            for(File file : streamRootDir.listFiles()){
                boolean deleted = file.delete();
                System.out.println("Deleted " + file. getName() + " : " + deleted);
            }
            boolean deleted = streamRootDir.delete();
            System.out.println("Deleted " + streamRootDir. getName() + " : " + deleted);

        }
    }

    @Test
    public void testToHex() {
        assertEquals("0000000000000000", HexUtil.toHex(0x0));
        assertEquals("000000000000000F", HexUtil.toHex(0xF));
        assertEquals("00000000000000FF", HexUtil.toHex(0xFF));
        assertEquals("FEDCBA9876543210", HexUtil.toHex(0xFEDCBA9876543210L));
    }
}
