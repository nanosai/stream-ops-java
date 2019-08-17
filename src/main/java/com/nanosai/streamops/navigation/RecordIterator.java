package com.nanosai.streamops.navigation;

import com.nanosai.rionops.rion.RionFieldTypes;
import com.nanosai.rionops.rion.read.RionReader;
import com.nanosai.streamops.rion.StreamOpsRionFieldTypes;
import com.nanosai.streamops.storage.file.StreamStorageFS;

public class RecordIterator {

    public long offset = -1;

    protected RionReader rionReader = new RionReader();

    public RecordIterator setSource(byte[] source, int sourceOffset, int sourceLength){
        this.rionReader.setSource(source, sourceOffset, sourceLength);
        return this;
    }

    public RecordIterator resetSource(int sourceOffset, int sourceLength){
        this.rionReader.setSource(this.rionReader.source, sourceOffset, sourceLength);
        return this;
    }

    public boolean hasNext() {
        return this.rionReader.hasNext();
    }

    public RionReader getRionReader() {
        return this.rionReader;
    }

    public void next() {
        this.rionReader.nextParse();
        this.offset++;

        while(this.rionReader.fieldType         == RionFieldTypes.EXTENDED &&
              this.rionReader.fieldTypeExtended == StreamOpsRionFieldTypes.OFFSET_EXTENDED_RION_TYPE){

            //read offset value of extended field as long
            this.offset = this.rionReader.readInt64();
            this.rionReader.nextParse();
        }

        // if field type == extended && extended field type == offset
        //    read offset into iterators offset variable.
        //    nextParse()
        //    repeat until not field type == extended && field type == offset
        // else increment offset variable by 1.
    }


}
