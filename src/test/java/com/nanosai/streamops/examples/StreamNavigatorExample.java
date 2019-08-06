package com.nanosai.streamops.examples;

import com.nanosai.streamops.navigation.RecordIterator;
import com.nanosai.streamops.navigation.StreamNavigator;
import com.nanosai.streamops.storage.file.StreamStorageFS;

import java.io.IOException;

public class StreamNavigatorExample {

    public static void main(String[] args) throws IOException {

        byte[] rionBytesRecord1 = new byte[]{0x01, 0x08, 0,1,2,3,4,5,6,7};

        StreamStorageFS streamStorage = new StreamStorageFS("navigator-example-stream","data/navigator-example-stream", 24);

        System.out.println("streamStorage.nextRecordOffset = " + streamStorage.nextRecordOffset);

        streamStorage.openForAppend();
        for(int i=0; i<6; i++){
            streamStorage.appendRecord(rionBytesRecord1, 0, rionBytesRecord1.length);
        }
        streamStorage.closeForAppend();

        StreamNavigator streamNavigator = new StreamNavigator();
        RecordIterator recordIterator = new RecordIterator();
        recordIterator.setSource(new byte[1024], 0, 0); //will be overridden in navigateTo() call later.

        long currentOffset = 0;

        while(currentOffset < streamStorage.nextRecordOffset){
            System.out.println("Navigating to: " + currentOffset);
            streamNavigator.navigateTo(streamStorage, currentOffset, recordIterator);

            System.out.println("-- Record offset: " + recordIterator.offset);
            while(recordIterator.hasNext()){
                recordIterator.next();
                System.out.println("-- Record offset: " + recordIterator.offset);
            }
            currentOffset = recordIterator.offset + 1;
        }



    }
}
