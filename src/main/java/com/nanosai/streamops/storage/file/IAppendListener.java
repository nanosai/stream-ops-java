package com.nanosai.streamops.storage.file;

import java.io.IOException;

/**
 * This interface should be implemented by classes interested in being notified when a stream (of some kind)
 * appends a record.
 */
public interface IAppendListener {

    /**
     * Called when the source this listener is listening to has successfully appended a record.
     *
     * @param source       The byte array containing the new RION record that was appended.
     * @param sourceOffset The offset into the byte array from which the RION record starts.
     * @param sourceLength The length of the RION record in bytes inside the byte array.
     * @param recordOffset The offset the record was given in the source stream.
     * @throws IOException
     */
    public void recordAppended(byte[] source, int sourceOffset, int sourceLength, long recordOffset) throws IOException;

}
