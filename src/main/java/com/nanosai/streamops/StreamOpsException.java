package com.nanosai.streamops;

public class StreamOpsException extends RuntimeException {

    public StreamOpsException() {
        super();
    }

    public StreamOpsException(String message) {
        super(message);
    }

    public StreamOpsException(String message, Throwable cause) {
        super(message, cause);
    }

    public StreamOpsException(Throwable cause) {
        super(cause);
    }

    protected StreamOpsException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
