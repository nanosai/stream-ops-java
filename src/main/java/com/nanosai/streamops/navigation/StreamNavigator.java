package com.nanosai.streamops.navigation;

import com.nanosai.streamops.StreamOpsException;
import com.nanosai.streamops.storage.file.StreamStorageBlockFS;
import com.nanosai.streamops.storage.file.StreamStorageFS;

import java.io.IOException;
import java.util.List;

public class StreamNavigator {


    public void navigateTo(StreamStorageFS streamStorageFS, long toOffset, RecordIterator iterator){

        //0. check if the requested start offset even exists in the stream
        if(toOffset >= streamStorageFS.nextRecordOffset){
            return; //No, it doesn't - nothing to iterate to.
        }

        // 1. Find the correct block file.
        StreamStorageBlockFS closestStorageBlock = findClosestStorageBlock(streamStorageFS, toOffset);

        System.out.println(closestStorageBlock.getFileName());

        // 2. Read the whole block into byte array.
        if(iterator.getRionReader().source.length < closestStorageBlock.getFileLength()){
            throw new StreamOpsException("Buffer inside RecordIterator too small to hold content of stream block file. " +
                    "Buffer was " + iterator.getRionReader().source.length + " bytes in length. " +
                    "Stream block file was " + closestStorageBlock.getFileLength() + " bytes in length. "
            );
        }
        int bytesRead = 0;
        try {
            bytesRead = streamStorageFS.readBytes(closestStorageBlock, 0, iterator.getRionReader().source, 0, (int) closestStorageBlock.getFileLength());
        } catch (IOException e) {
            throw new StreamOpsException("Error reading stream block file [" + closestStorageBlock.getFileName() + "]: "
                    + e.getMessage(), e);
        }

        iterator.resetSource(0, bytesRead);

        // 3. Iterate through RecordIterator until toOffset is found

        while(iterator.hasNext() && iterator.offset != toOffset){
            iterator.next();
        }


    }

    protected StreamStorageBlockFS findClosestStorageBlock(StreamStorageFS streamStorageFS, long toOffset) {
        List<StreamStorageBlockFS> storageBlocks = streamStorageFS.getStorageBlocks();
        StreamStorageBlockFS closestStorageBlock = null;
        for(int i=0; i<storageBlocks.size(); i++){
            StreamStorageBlockFS storageBlock = storageBlocks.get(i);
            if(storageBlock.getFirstOffset() <= toOffset){
                closestStorageBlock = storageBlock;
            } else {
                break;
            }
        }
        return closestStorageBlock;
    }

}
