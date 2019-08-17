package com.nanosai.streamops.examples;

import com.nanosai.rionops.rion.RionFieldTypes;
import com.nanosai.rionops.rion.read.RionReader;
import com.nanosai.rionops.rion.write.RionWriter;
import com.nanosai.streamops.rion.StreamOpsRionFieldTypes;
import com.nanosai.streamops.storage.file.StreamStorageFS;
import com.nanosai.streamops.util.FileUtil;

import java.io.File;
import java.io.IOException;

public class FullStreamIterationExample {

    public static void main(String[] args) throws IOException {
        String streamId  = "full-stream-iteration-example-1";
        String streamDir = "data/" + streamId;
        FileUtil.resetDir(new File(streamDir));

        byte[] rionRecord = new byte[1024];
        RionWriter rionWriter = new RionWriter()
                .setNestedFieldStack(new int[16])
                .setDestination(rionRecord,0);

        rionWriter.writeObjectBeginPush(2);
        rionWriter.writeUtf8("Value1");
        rionWriter.writeUtf8("Value2");
        //rionWriter.writeUtf8("Value3");
        rionWriter.writeObjectEndPop();

        //byte[] rionBytesRecord1 = new byte[]{0x01, 0x08, 0,1,2,3,4,5,6,7}; //10 bytes in total, 8 bytes in the RION Bytes field body

        StreamStorageFS streamStorage = new StreamStorageFS(streamId, streamDir, 1024);
        appendRecordsToStream(rionRecord, rionWriter.index, streamStorage);


        byte[] recordBuffer = new byte[(int) streamStorage.getStorageFileBlockMaxSize()];
        RionReader rionReader = new RionReader();
        long recordOffset = 0;

        for(int i=0, n = streamStorage.getStorageBlocks().size(); i<n; i++){
            int lengthRead = streamStorage.readFromBlockWithIndex(i,0, recordBuffer, 0, recordBuffer.length);

            rionReader.setSource(recordBuffer, 0, lengthRead);

            while(rionReader.hasNext()){
                rionReader.nextParse();

                if(rionReader.fieldType         == RionFieldTypes.EXTENDED &&
                   rionReader.fieldTypeExtended == StreamOpsRionFieldTypes.OFFSET_EXTENDED_RION_TYPE){
                    //this is a record offset field - read record offset value
                    recordOffset = rionReader.readInt64();
                } else {
                    //this is a record field - read record value
                    System.out.println("[" + recordOffset + "][" + rionReader.fieldType +"]["+rionReader.fieldLength +"]");

                    rionReader.moveInto();
                    while(rionReader.hasNext()){
                        rionReader.nextParse();
                        System.out.println("   [" + recordOffset + "][" + rionReader.fieldType +"]["+ rionReader.readUtf8String() +"]");
                    }
                    rionReader.moveOutOf();

                    //after processing this record, increment recordOffset for next record - in case no explicit record offset field is found
                    recordOffset++;
                }
            }

        }

    }

    private static void appendRecordsToStream(byte[] rionBytesRecord1, int length, StreamStorageFS streamStorageFS) throws IOException {
        streamStorageFS.openForAppend();
        streamStorageFS.appendRecord(rionBytesRecord1, 0, length);
        streamStorageFS.appendRecord(rionBytesRecord1, 0, length);
        streamStorageFS.appendRecord(rionBytesRecord1, 0, length);

        streamStorageFS.nextRecordOffset += 10;
        streamStorageFS.appendOffset();
        streamStorageFS.appendRecord(rionBytesRecord1, 0, length);
        streamStorageFS.appendRecord(rionBytesRecord1, 0, length);
        streamStorageFS.appendRecord(rionBytesRecord1, 0, length);


        streamStorageFS.closeForAppend();
    }
}
