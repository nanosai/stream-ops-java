package com.nanosai.streamops.storage.file;

import com.nanosai.rionops.rion.read.RionReader;

public interface IRecordProcessor {

    public boolean process(long recordOffset, RionReader rionReader);

}
