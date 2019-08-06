package com.nanosai.streamops;

import com.nanosai.streamops.navigation.RecordIterator;
import com.nanosai.streamops.storage.StreamStorageFactory;

public class StreamOps {


    public static RecordIterator createRecordIterator(){
        return new RecordIterator();
    }

    public static StreamStorageFactory createStreamStorageFactory() {
        return new StreamStorageFactory();
    }


}
