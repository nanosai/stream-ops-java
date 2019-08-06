package com.nanosai.streamops.navigation;

import com.nanosai.streamops.storage.file.StreamStorageFS;
import com.nanosai.streamops.util.FileUtil;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class StreamIteratorTest {


    @Test
    public void test() throws IOException {
        FileUtil.deleteDir("data/stream-id");
        StreamStorageFS streamStorage = new StreamStorageFS("stream-id", "data/stream-id", 24);

        byte[] rionBytesRecord = new byte[]{0x01, 0x08, 0,1,2,3,4,5,6,7}; //10 bytes in total, 8 bytes in the RION Bytes field body

        streamStorage.openForAppend();
        streamStorage.appendRecord(rionBytesRecord, 0, rionBytesRecord.length);
        streamStorage.appendRecord(rionBytesRecord, 0, rionBytesRecord.length);
        streamStorage.appendRecord(rionBytesRecord, 0, rionBytesRecord.length);
        streamStorage.appendRecord(rionBytesRecord, 0, rionBytesRecord.length);
        streamStorage.closeForAppend();


        StreamNavigator streamNavigator = new StreamNavigator();
        RecordIterator recordIterator = new RecordIterator();
        recordIterator.setSource(new byte[1024], 0, 24);
        streamNavigator.navigateTo(streamStorage, 2, recordIterator);

        assertEquals(2, recordIterator.offset);

        assertTrue(recordIterator.hasNext());
        recordIterator.next();
        assertEquals(3, recordIterator.offset);
        assertFalse(recordIterator.hasNext());
    }
}
