package com.nanosai.streamops.engine.storage.file;

import com.nanosai.streamops.rion.RionUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class StreamFileStorage {

    private String streamId    = null;
    private String rootDirPath = null;

    private   List<StreamFileStorageBlock> storageBlocks = new ArrayList<>();

    private   StreamFileStorageBlock latestBlock             = null;
    private   FileOutputStream       latestBlockOutputStream = null;

    private long storageFileBlockMaxSize = 1024 * 1024;

    public long nextRecordOffset = 0;

    protected byte[] offsetRionBuffer = new byte[16];

    public StreamFileStorage(String streamId, String rootDirPath) throws IOException {
        this.streamId    = streamId;
        this.rootDirPath = rootDirPath;

        syncFromDisk();
    }

    public StreamFileStorage(String streamId, String rootDirPath, long storageFileBlockMaxSize) throws IOException {
        this.streamId    = streamId;
        this.rootDirPath = rootDirPath;
        this.storageFileBlockMaxSize = storageFileBlockMaxSize;

        syncFromDisk();
    }

    public long getStorageFileBlockMaxSize() {
        return this.storageFileBlockMaxSize;
    }

    public StreamFileStorageBlock getLatestBlock() {
        return this.latestBlock;
    }

    public List<StreamFileStorageBlock> getStorageBlocks() {
        return storageBlocks;
    }

    protected String createNewStreamBlockFileName() {
        return this.streamId + "-" + toHex(nextRecordOffset);
    }

    protected static String toHex(long offset) {
        StringBuilder builder = new StringBuilder();

        for(int i=60; i >= 0; i-=4){
            char digitChar='0';
            long digit = offset >> i;
            digit &= 0xF;
            switch((int) digit) {
                case 0 : digitChar = '0'; break;
                case 1 : digitChar = '1'; break;
                case 2 : digitChar = '2'; break;
                case 3 : digitChar = '3'; break;
                case 4 : digitChar = '4'; break;
                case 5 : digitChar = '5'; break;
                case 6 : digitChar = '6'; break;
                case 7 : digitChar = '7'; break;
                case 8 : digitChar = '8'; break;
                case 9 : digitChar = '9'; break;
                case 10 : digitChar = 'A'; break;
                case 11 : digitChar = 'B'; break;
                case 12 : digitChar = 'C'; break;
                case 13 : digitChar = 'D'; break;
                case 14 : digitChar = 'E'; break;
                case 15 : digitChar = 'F'; break;
            }
            builder.append(digitChar);
        }

        return builder.toString();
    }

    protected void syncFromDisk() throws IOException {
        File rootDir = new File(this.rootDirPath);
        File[] files = rootDir.listFiles();
        for(File file : files) {
            String fileName = file.getName();
            long firstOffset = Long.parseLong(fileName.substring(fileName.lastIndexOf("-") + 1, fileName.length()));
            long fileLength  = file.length();
            StreamFileStorageBlock fileStorageBlock = new StreamFileStorageBlock(file.getName(),fileLength, firstOffset);
            this.storageBlocks.add(fileStorageBlock);
        }
        if(this.storageBlocks.size() > 0) {
            this.latestBlock = storageBlocks.get(storageBlocks.size()-1);
        } else {
            this.latestBlock = new StreamFileStorageBlock(createNewStreamBlockFileName(), 0, 0);
            openForAppend();
            appendOffset();
            closeForAppend();
            this.storageBlocks.add(this.latestBlock);
        }

    }

    public void openForAppend() throws FileNotFoundException {
        this.latestBlockOutputStream = new FileOutputStream(this.rootDirPath + "/" + this.latestBlock.getFileName(), true);
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
        this.latestBlock.fileLength += lengthOfOffset + 2;
    }

    public void appendRecord(byte[] source, int sourceOffset, int sourceLength) throws IOException {
        if(this.latestBlock.fileLength + sourceLength > storageFileBlockMaxSize){
            //create a new latest block
            closeForAppend();
            StreamFileStorageBlock newStreamFileStorageBlock = new StreamFileStorageBlock(createNewStreamBlockFileName(), 0, this.nextRecordOffset);
            this.storageBlocks.add(newStreamFileStorageBlock);
            this.latestBlock = newStreamFileStorageBlock;
            openForAppend();
        }
        if(this.latestBlock.fileLength == 0){
            appendOffset();
        }

        // appendRecord data to file
        this.latestBlockOutputStream.write(source, sourceOffset, sourceLength);
        this.latestBlock.fileLength += sourceLength;
        this.nextRecordOffset++;
    }

    public void closeForAppend() throws IOException {
        this.latestBlockOutputStream.close();
    }


    public int readStreamFileStorageBlock(StreamFileStorageBlock streamFileStorageBlock, long fromByte, byte[] dest, int destOffset, int length) throws IOException {

        try(RandomAccessFile randomAccessFile = new RandomAccessFile(this.rootDirPath + "/" + streamFileStorageBlock.getFileName(), "r")){
            randomAccessFile.seek(fromByte);

            return  randomAccessFile.read(dest, destOffset, length);
        }
    }




    public static class ReadResult {
        public int  firstRecordByteOffset;   // byte offset into block of bytes where first record with requested offset starts.
        public long lastRecordOffset;        // record offset in stream of last record read into block

    }
}
