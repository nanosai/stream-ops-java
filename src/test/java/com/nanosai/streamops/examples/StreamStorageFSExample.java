package com.nanosai.streamops.examples;

import com.nanosai.streamops.navigation.RecordIterator;
import com.nanosai.streamops.storage.file.StreamStorageBlockFS;
import com.nanosai.streamops.storage.file.StreamStorageFS;
import com.nanosai.streamops.util.FileUtil;

import java.io.IOException;

public class StreamStorageFSExample {

    public static void main(String[] args) throws IOException {
        FileUtil.deleteDir("data/stream-id");

        //StreamStorageFactory streamStorageFactory = StreamOps.createStreamStorageFactory();
        //StreamStorageFS streamStorageFS = streamStorageFactory.createStreamStorageFS("stream-id", "data/stream-id");
        StreamStorageFS streamStorageFS = new StreamStorageFS("stream-id", "data/stream-id");

        writeRecordsToStream(streamStorageFS);
        readRecordsFromStream(streamStorageFS);
    }

    private static void readRecordsFromStream(StreamStorageFS streamStorageFS) throws IOException {

        byte[] recordBuffer = new byte[1024];

        StreamStorageBlockFS streamStorageBlockFS = streamStorageFS.getStorageBlocks().get(0);

        int lengthRead = streamStorageFS.readBytes(streamStorageBlockFS, 0,
                recordBuffer, 0, (int) streamStorageBlockFS.getFileLength());

        RecordIterator recordIterator = new RecordIterator();
        recordIterator.setSource(recordBuffer, 0, lengthRead);

        while(recordIterator.hasNext()){
            recordIterator.next();

            System.out.println(recordIterator.offset);
        }

    }


    private static void writeRecordsToStream(StreamStorageFS streamStorageFS) throws IOException {
        //10 bytes in total, 8 bytes in the RION Bytes field body
        byte[] rionBytesRecord1 = new byte[]{0x01, 0x08, 0,1,2,3,4,5,6,7};
        byte[] rionBytesRecord2 = new byte[]{0x01, 0x08, 7,6,5,4,3,2,1,0};
        byte[] rionBytesRecord3 = new byte[]{0x01, 0x08, 0,1,2,3,3,2,1,0};


        streamStorageFS.openForAppend();
        streamStorageFS.appendRecord(rionBytesRecord1, 0, rionBytesRecord1.length);
        streamStorageFS.appendRecord(rionBytesRecord2, 0, rionBytesRecord2.length);
        streamStorageFS.appendRecord(rionBytesRecord3, 0, rionBytesRecord3.length);
        streamStorageFS.closeForAppend();
    }


}
