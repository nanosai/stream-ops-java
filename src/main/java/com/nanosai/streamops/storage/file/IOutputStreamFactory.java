package com.nanosai.streamops.storage.file;

import java.io.IOException;
import java.io.OutputStream;

public interface IOutputStreamFactory {


    public OutputStream createOutputStream(String filePath) throws IOException;

}
