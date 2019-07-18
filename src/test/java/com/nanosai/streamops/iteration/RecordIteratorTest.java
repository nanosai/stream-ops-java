package com.nanosai.streamops.iteration;

import com.nanosai.streamops.iteration.RecordIterator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class RecordIteratorTest {



    @Test
    public void test() {
        byte[] buffer = new byte[1024];

        buffer[0] =  (byte) (255 & (0xF1));
        buffer[1] =  (byte) 1;  //extended type 1 = record offset
        buffer[2] =  (byte) 1;  //length in bytes of offset is 1 byte
        buffer[3] =  (byte) 0;  //offset is 0.

        buffer[4] =  (byte) (255 &(0x01)); //Bytes RION field
        buffer[5] =  (byte) 1; //Bytes field length = 1 byte
        buffer[6] =  (byte) 66;

        buffer[7] =  (byte) (255 &(0x01)); //Bytes RION field
        buffer[8] =  (byte) 1; //Bytes field length = 1 byte
        buffer[9] =  (byte) 77;

        buffer[10] =  (byte) (255 & (0xF1));
        buffer[11] =  (byte) 1;  //extended type 1 = record offset
        buffer[12] =  (byte) 1;  //length in bytes of offset is 1 byte
        buffer[13] =  (byte) 5;  //offset is 0.

        buffer[14] =  (byte) (255 &(0x01)); //Bytes RION field
        buffer[15] =  (byte) 1; //Bytes field length = 1 byte
        buffer[16] =  (byte) 99;

        RecordIterator recordIterator = new RecordIterator();
        recordIterator.setSource(buffer, 0, 17);

        assertTrue(recordIterator.hasNext());
        recordIterator.next();
        assertEquals(0, recordIterator.offset);
        assertEquals(66, recordIterator.getRionReader().source[recordIterator.rionReader.index]);

        recordIterator.next();
        assertEquals(1, recordIterator.offset);
        assertEquals(77, recordIterator.getRionReader().source[recordIterator.rionReader.index]);

        assertTrue(recordIterator.hasNext());
        recordIterator.next();
        assertEquals(5, recordIterator.offset);
        assertEquals(99, recordIterator.getRionReader().source[recordIterator.rionReader.index]);

        assertFalse(recordIterator.hasNext());



    }
}