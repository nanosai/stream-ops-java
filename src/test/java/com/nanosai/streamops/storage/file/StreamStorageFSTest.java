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

        String streamRootDir = "data/test-stream-fs2-1";
        FileUtil.resetDir(new File(streamRootDir));

        StreamStorageFS streamStorageFS = new StreamStorageFS("test-stream-fs2-1", streamRootDir);

        assertNotNull(streamStorageFS.getLatestBlock());
        assertEquals("test-stream-fs2-1-0000000000000000", streamStorageFS.getLatestBlock().getFileName());
        assertEquals(1, streamStorageFS.getStorageBlocks().size());

        assertTrue(new File(streamRootDir).exists());
    }

    @Test
    public void testAppendToStreamFileStorage() throws IOException {
        String streamRootDir = "data/test-stream-fs2-1";
        FileUtil.resetDir(new File(streamRootDir));
        //initStreamDir(streamRootDir);

        StreamStorageFS streamStorageFS = new StreamStorageFS("test-stream-fs2-1", streamRootDir);

        byte[] rionBytesRecord = new byte[]{0x01, 0x08, 0,1,2,3,4,5,6,7}; //10 bytes in total, 8 bytes in the RION Bytes field body

        //todo next step is to test append, and see that files are created and data written to it.
        streamStorageFS.openForAppend();
        streamStorageFS.appendRecord(rionBytesRecord, 0, rionBytesRecord.length);
        streamStorageFS.closeForAppend();

        assertEquals(10, streamStorageFS.getLatestBlock().fileLength);

        streamStorageFS.openForAppend();
        streamStorageFS.appendRecord(rionBytesRecord, 0, rionBytesRecord.length);
        streamStorageFS.closeForAppend();

        assertEquals(20, streamStorageFS.getLatestBlock().fileLength);

    }

    @Test
    public void testAppendListenerCalled() throws IOException {
        String streamRootDir = "data/test-stream-fs2-1";
        FileUtil.resetDir(new File(streamRootDir));

        StreamStorageFS streamStorageFS = new StreamStorageFS("test-stream-fs2-1", streamRootDir, 20);

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
        String streamRootDir = "data/test-stream-fs2-1";
        FileUtil.resetDir(new File(streamRootDir));
        //initStreamDir("data/test-stream-1");

        StreamStorageFS streamStorageFS = new StreamStorageFS("test-stream-1", streamRootDir, 20);

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

        assertEquals(2, streamStorageFS.getStorageBlocks().size());
        assertEquals(20, streamStorageFS.getStorageBlocks().get(0).fileLength);
        assertEquals(12, streamStorageFS.getStorageBlocks().get(1).fileLength);

        byte[] dest1 = new byte[20];

        int bytesRead1 = streamStorageFS.readFromBlock(
                streamStorageFS.getStorageBlocks().get(0), 0, dest1, 0, 20);

        assertEquals(20, bytesRead1);

        assertEquals(0x01, 255 & dest1[0]); // lead byte of RION Bytes field - with length byte count 1
        assertEquals(0x08, 255 & dest1[1]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes

        assertEquals(0x00, 255 & dest1[2]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x01, 255 & dest1[3]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x02, 255 & dest1[4]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x03, 255 & dest1[5]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x04, 255 & dest1[6]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x05, 255 & dest1[7]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x06, 255 & dest1[8]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x07, 255 & dest1[9]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes

        assertEquals(0x01, 255 & dest1[10]); // lead byte of RION Bytes field - with length byte count 1
        assertEquals(0x08, 255 & dest1[11]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes

        assertEquals(0x00, 255 & dest1[12]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x01, 255 & dest1[13]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x02, 255 & dest1[14]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x03, 255 & dest1[15]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x04, 255 & dest1[16]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x05, 255 & dest1[17]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x06, 255 & dest1[18]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x07, 255 & dest1[19]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes

        byte[] dest2 = new byte[20];

        int bytesRead2 = streamStorageFS.readFromBlock(
                streamStorageFS.getStorageBlocks().get(1), 0, dest2, 0, 20);

        assertEquals(12, bytesRead2);


        assertEquals(0x01, 255 & dest2[0]); // lead byte of RION Bytes field - with length byte count 1
        assertEquals(0x04, 255 & dest2[1]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes

        assertEquals(0x00, 255 & dest2[2]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x01, 255 & dest2[3]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x02, 255 & dest2[4]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x03, 255 & dest2[5]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes

        assertEquals(0x01, 255 & dest2[6]); // lead byte of RION Bytes field - with length byte count 1
        assertEquals(0x04, 255 & dest2[7]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes

        assertEquals(0x00, 255 & dest2[8]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x02, 255 & dest2[9]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x04, 255 & dest2[10]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes
        assertEquals(0x06, 255 & dest2[11]); // length byte of RION Bytes field. Total length of RION Bytes field body is 8 bytes


    }


    @Test
    public void testIterateFrom() throws IOException {
        FileUtil.resetDir(new File("data/test-stream-1"));
        StreamStorageFS streamStorageFS = new StreamStorageFS("test-stream-1", "data/test-stream-1", 30);

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


        byte[] recordBuffer = new byte[30];
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
