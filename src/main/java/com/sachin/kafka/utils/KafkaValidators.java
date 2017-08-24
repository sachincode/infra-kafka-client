package com.sachin.kafka.utils;

import com.sachin.kafka.exception.InfraKafkaException;
import org.apache.commons.lang.StringUtils;

import java.util.regex.Pattern;

/**
 * kafka主题和消息体的有效性检查
 * 
 * @author shicheng.zhang
 * @since 17-8-24 下午8:25
 */
public class KafkaValidators {

    private KafkaValidators() {
    }

    private static Pattern TOPIC_PATTERN = Pattern.compile("^[a-zA-Z0-9_-]+$");

    public static final int CHARACTER_MAX_LENGTH = 255;

    /**
     * topic 有效性检查
     *
     * @param topic
     * @throws InfraKafkaException
     */
    public static void checkTopic(String topic) throws InfraKafkaException {
        if (StringUtils.isBlank(topic)) {
            throw new InfraKafkaException("the specified topic is blank", null);
        }
        if (!TOPIC_PATTERN.matcher(topic).matches()) {
            throw new InfraKafkaException(String.format("the specified topic[%s] contains illegal characters", topic));
        }
        if (topic.length() > CHARACTER_MAX_LENGTH) {
            throw new InfraKafkaException("the specified topic is longer than topic max length 255.");
        }
    }

    /**
     * msgData有效性检查
     *
     * @param msgData
     * @throws InfraKafkaException
     */
    public static void checkMsgData(String msgData) throws InfraKafkaException {
        if (null == msgData) {
            throw new InfraKafkaException("the message is null", null);
        }
        if (0 == msgData.length()) {
            throw new InfraKafkaException("the message body length is zero", null);
        }
    }

    /**
     * message 有效性检查 (topic 和 msgData)
     *
     * @param topic
     * @param msgData
     * @throws InfraKafkaException
     */
    public static void checkMessage(String topic, String msgData) throws InfraKafkaException {
        checkTopic(topic);
        checkMsgData(msgData);
    }

}
