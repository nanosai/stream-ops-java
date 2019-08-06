package com.nanosai.streamops.examples;

import com.nanosai.streamops.navigation.RecordIterator;
import com.nanosai.streamops.storage.file.StreamStorageBlockFS;
import com.nanosai.streamops.storage.file.StreamStorageFS;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class RecordIterationExample {

    public static void main(String[] args) throws IOException {

        initStreamDir("data/example-stream-1");

        // write some records to a stream
        byte[] rionBytesRecord1 = new byte[]{0x01, 0x08, 0,1,2,3,4,5,6,7}; //10 bytes in total, 8 bytes in the RION Bytes field body

        StreamStorageFS streamStorageFS = new StreamStorageFS("example-stream-1", "data/example-stream-1", 1024);
        appendRecordsToStream(rionBytesRecord1, streamStorageFS);

        // read all records into a byte array
        byte[] records = new byte[1024];

        List<StreamStorageBlockFS> storageBlocks = streamStorageFS.getStorageBlocks();
        StreamStorageBlockFS streamStorageBlockFS = storageBlocks.get(0);

        long fileLength = streamStorageBlockFS.getFileLength();

        int bytesRead = streamStorageFS.readBytes(streamStorageBlockFS, 0, records, 0,  (int) fileLength);


        // attach a RionReader to the byte array
        //RecordIterator recordIterator= StreamOps.createRecordIterator();
        RecordIterator recordIterator = new RecordIterator();
        recordIterator.setSource(records, 0, bytesRead);

        // iterate records
        while(recordIterator.hasNext()){
            recordIterator.next();

            System.out.println("record offset: " + recordIterator.offset);

        }
    }

    private static void appendRecordsToStream(byte[] rionBytesRecord1, StreamStorageFS streamStorageFS) throws IOException {
        streamStorageFS.openForAppend();
        streamStorageFS.appendRecord(rionBytesRecord1, 0, rionBytesRecord1.length);
        streamStorageFS.appendRecord(rionBytesRecord1, 0, rionBytesRecord1.length);
        streamStorageFS.appendRecord(rionBytesRecord1, 0, rionBytesRecord1.length);
        streamStorageFS.closeForAppend();
    }

    private static void initStreamDir(String streamRootDirPath) {
        deleteDir(streamRootDirPath);
        new File(streamRootDirPath).mkdirs();
    }

    private static void deleteDir(String dirPath) {
        File streamRootDir = new File(dirPath);
        if(streamRootDir.exists()) {
            for(File file : streamRootDir.listFiles()){
                file.delete();
            }
            streamRootDir.delete();
        }
    }
}
