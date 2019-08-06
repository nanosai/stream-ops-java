package com.nanosai.streamops.storage.file;

import com.nanosai.streamops.navigation.RecordIterator;
import com.nanosai.streamops.rion.RionUtil;
import com.nanosai.streamops.util.HexUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class StreamStorageFS {

    public  static final int OFFSET_EXTENDED_RION_TYPE = 1;

    private String streamId    = null;
    private String rootDirPath = null;

    private   List<StreamStorageBlockFS> storageBlocks = new ArrayList<>();

    private StreamStorageBlockFS latestBlock             = null;
    private   FileOutputStream   latestBlockOutputStream = null;

    private long storageFileBlockMaxSize = 1024 * 1024;

    public long nextRecordOffset = 0;

    protected byte[] offsetRionBuffer = new byte[16];

    public StreamStorageFS(String streamId, String rootDirPath) throws IOException {
        this.streamId    = streamId;
        this.rootDirPath = rootDirPath;

        syncFromDisk();
    }

    public StreamStorageFS(String streamId, String rootDirPath, long storageFileBlockMaxSize) throws IOException {
        this.streamId    = streamId;
        this.rootDirPath = rootDirPath;
        this.storageFileBlockMaxSize = storageFileBlockMaxSize;

        syncFromDisk();
    }

    public String getStreamId() {
        return streamId;
    }

    public String getRootDirPath() {
        return rootDirPath;
    }

    public long getStorageFileBlockMaxSize() {
        return this.storageFileBlockMaxSize;
    }

    public StreamStorageBlockFS getLatestBlock() {
        return this.latestBlock;
    }

    public List<StreamStorageBlockFS> getStorageBlocks() {
        return storageBlocks;
    }

    protected String createNewStreamBlockFileName() {
        return this.streamId + "-" + HexUtil.toHex(nextRecordOffset);
    }



    protected void syncFromDisk() throws IOException {
        File rootDir = new File(this.rootDirPath);
        if(!rootDir.exists()){
            rootDir.mkdirs();
        }
        File[] files = rootDir.listFiles();
        for(File file : files) {
            String fileName = file.getName();
            //long firstOffset = Long.parseLong(fileName.substring(fileName.lastIndexOf("-") + 1, fileName.length()));
            long firstOffset = HexUtil.fromHex(fileName.substring(fileName.lastIndexOf("-") + 1, fileName.length()));
            long fileLength  = file.length();
            StreamStorageBlockFS fileStorageBlock = new StreamStorageBlockFS(file.getName(),fileLength, firstOffset);
            this.storageBlocks.add(fileStorageBlock);
        }

        Collections.sort(this.storageBlocks, new Comparator<StreamStorageBlockFS>() {
            @Override
            public int compare(StreamStorageBlockFS block0, StreamStorageBlockFS block1) {
                if (block0.getFirstOffset() < block1.getFirstOffset()) {
                    return -1;
                }
                if (block0.getFirstOffset() > block1.getFirstOffset()) {
                    return 1;
                }
                return 0;
            }
        });

        if(this.storageBlocks.size() > 0) {
            this.latestBlock = storageBlocks.get(storageBlocks.size()-1);
            //todo find latest offset in that storage block, and set nextRecordOffset to that latest offset + 1
            byte[] latestBlockBytes = new byte[(int) this.latestBlock.getFileLength()];
            readBytes(this.latestBlock, 0, latestBlockBytes, 0, latestBlockBytes.length);

            RecordIterator recordIterator = new RecordIterator();
            recordIterator.setSource(latestBlockBytes, 0, latestBlockBytes.length);

            while(recordIterator.hasNext()){
                recordIterator.next();
            }
            this.nextRecordOffset = recordIterator.offset + 1;

        } else {
            this.latestBlock = new StreamStorageBlockFS(createNewStreamBlockFileName(), 0, 0);
            openForAppend();
            appendOffset();
            closeForAppend();
            this.storageBlocks.add(this.latestBlock);
        }

    }

    public void openForAppend() throws FileNotFoundException {
        String latestBlockFilePath = this.rootDirPath + "/" + this.latestBlock.getFileName();
        this.latestBlockOutputStream = new FileOutputStream(latestBlockFilePath, true);
    }

    public void incNextRecordOffset() {
        this.nextRecordOffset++;
    }

    public void appendOffset() throws IOException {
        int lengthOfOffset = (255 & RionUtil.lengthOfInt64Value(this.nextRecordOffset));
        byte rionExtendedFieldLeadByte = (byte) ((255 & (15 << 4)) | lengthOfOffset);
        byte rionStreamOffsetType      = (byte) OFFSET_EXTENDED_RION_TYPE;

        this.offsetRionBuffer[0] = rionExtendedFieldLeadByte;
        this.offsetRionBuffer[1] = rionStreamOffsetType;
        this.offsetRionBuffer[2] = (byte) (255 & lengthOfOffset);

        int index = 3;
        for(int i=(lengthOfOffset-1)*8; i >= 0; i-=8){
            this.offsetRionBuffer[index++] = (byte) (255 & (this.nextRecordOffset >> i));
        }
        this.latestBlockOutputStream.write(this.offsetRionBuffer, 0, lengthOfOffset + 3);
        this.latestBlock.fileLength += lengthOfOffset + 2;
    }

    public void appendRecord(byte[] source, int sourceOffset, int sourceLength) throws IOException {
        if(this.latestBlock.fileLength + sourceLength > storageFileBlockMaxSize){
            //create a new latest block
            closeForAppend();
            StreamStorageBlockFS newStreamStorageBlockFS = new StreamStorageBlockFS(createNewStreamBlockFileName(), 0, this.nextRecordOffset);
            this.storageBlocks.add(newStreamStorageBlockFS);
            this.latestBlock = newStreamStorageBlockFS;
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


    public int readBytes(StreamStorageBlockFS streamStorageBlockFS, long fromByte, byte[] dest, int destOffset, int length) throws IOException {

        try(RandomAccessFile randomAccessFile = new RandomAccessFile(this.rootDirPath + "/" + streamStorageBlockFS.getFileName(), "r")){
            randomAccessFile.seek(fromByte);

            return  randomAccessFile.read(dest, destOffset, length);
        }
    }




    public static class ReadResult {
        public int  firstRecordByteOffset;   // byte offset into block of bytes where first record with requested offset starts.
        public long lastRecordOffset;        // record offset in stream of last record read into block

    }
}
