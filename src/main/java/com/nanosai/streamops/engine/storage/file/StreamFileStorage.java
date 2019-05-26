package com.nanosai.streamops.engine.storage.file;

import com.nanosai.streamops.rion.RionUtil;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StreamFileStorage {

    private String streamId    = null;
    private String rootDirPath = null;

    private List<StreamFileStorageBlock> storageBlocks = new ArrayList<>();

    private StreamFileStorageBlock latestBlock             = null;
    private FileOutputStream       latestBlockOutputStream = null;

    private long storageFileBlockMaxSize = 1024 * 1024;

    public long nextRecordOffset = 0;

    protected byte[] offsetRionBuffer = new byte[16];

    public StreamFileStorage(String streamId, String rootDirPath){
        this.streamId    = streamId;
        this.rootDirPath = rootDirPath;
    }

    protected String createNewStreamBlockFileName() {
        return this.rootDirPath + "/" + this.streamId + "-" + nextRecordOffset;
    }

    protected void createNewStreamBlock(long firstRecordOffsetOfBlock) {
        String newStreamBlockFileName = createNewStreamBlockFileName();
        StreamFileStorageBlock newStreamBlock = new StreamFileStorageBlock(newStreamBlockFileName, 0, firstRecordOffsetOfBlock);
        this.storageBlocks.add(newStreamBlock);
    }

    public void syncFromDisk() {
        //todo read the stream block files from disk, instead of creating a new instance as below
        this.latestBlock = new StreamFileStorageBlock(createNewStreamBlockFileName(), 0, this.nextRecordOffset);
    }

    public void openForAppend() throws FileNotFoundException {
        this.latestBlockOutputStream = new FileOutputStream(this.latestBlock.getFilePath(), true);
    }

    public void incNextRecordOffset() {
        this.nextRecordOffset++;
    }

    public void appendOffset() throws IOException {
        int lengthOfOffset = (255 & RionUtil.lengthOfInt64Value(this.nextRecordOffset));
        byte rionExtendedFieldLeadByte = (byte) ((255 & (15 << 4)) | lengthOfOffset);
        byte rionStreamOffsetType      = (byte) 1;

        this.offsetRionBuffer[0] = rionExtendedFieldLeadByte;
        this.offsetRionBuffer[1] = rionStreamOffsetType;

        int index = 2;
        for(int i=(lengthOfOffset-1)*8; i >= 0; i-=8){
            this.offsetRionBuffer[index++] = (byte) (255 & (this.nextRecordOffset >> i));
        }
        this.latestBlockOutputStream.write(this.offsetRionBuffer, 0, lengthOfOffset + 2);
    }

    public void appendRecord(byte[] source, int sourceOffset, int sourceLength) throws IOException {
        if(this.latestBlock.size + sourceLength > storageFileBlockMaxSize){
            //create a new latest block
            closeForAppend();
            createNewStreamBlock(sourceOffset);
            openForAppend();
        }

        // appendRecord data to file
        this.latestBlockOutputStream.write(source, sourceOffset, sourceLength);
    }

    public void closeForAppend() throws IOException {
        this.latestBlockOutputStream.close();
    }


}
