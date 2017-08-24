package com.sachin.kafka.exception;

/**
 * @author shicheng.zhang
 * @since 17-8-24 下午8:25
 */
public class InfraKafkaException extends Exception {

    private static final long serialVersionUID = 1L;

    private final int responseCode;

    private final String errorMessage;

    public InfraKafkaException(Exception e) {
        super(e);
        this.responseCode = -1;
        this.errorMessage = "InfraKafkaException";
    }

    public InfraKafkaException(String message) {
        super(message);
        this.responseCode = -1;
        this.errorMessage = message;
    }

    public InfraKafkaException(String message, Throwable e) {
        super(message, e);
        this.responseCode = -1;
        this.errorMessage = message;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

}
