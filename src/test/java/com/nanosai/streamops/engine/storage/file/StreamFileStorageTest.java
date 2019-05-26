package com.nanosai.streamops.engine.storage.file;

import org.junit.jupiter.api.Test;

import java.io.IOException;

public class StreamFileStorageTest {

    @Test
    public void test() throws IOException {
        StreamFileStorage streamFileStorage = new StreamFileStorage("test-stream", "data");


        //streamFileStorage.incNextRecordOffset();
        streamFileStorage.nextRecordOffset = 0xFFFF;
        streamFileStorage.appendOffset();

        streamFileStorage.appendRecord(new byte[]{0,1,2,3,4,5,6,7}, 0, 8);

    }
}
