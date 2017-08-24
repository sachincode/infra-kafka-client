package com.sachin.kafka.utils;

/**
 * @author shicheng.zhang
 * @since 17-8-24 下午8:24
 */
public class KafkaConstants {

    private KafkaConstants() {
    }

    public static final String KAFKA_CONFIG_FILE = "kafka-config.properties";

    public static final long CONSUMER_RECONNECT_INTERVAL_MS = 10000; // 10sec

    public static final String KAFKA_DEFAULT_ENCODING = "UTF-8";

    // 是否开启记录发送失败信息的功能
    public static final String SEND_FAIL_MESSAGE_RECORD_ENABLE = "producer.failed.message.journal.enable";

    // 存放发送失败信息的本地路径
    public static final String SEND_FAIL_MESSAGE_RECORD_FILE = "producer.failed.message.journal.file";
}
