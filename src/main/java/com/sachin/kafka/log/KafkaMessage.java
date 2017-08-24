package com.sachin.kafka.log;

import java.util.LinkedList;
import java.util.List;

/**
 * @author shicheng.zhang
 * @since 17-8-24 下午8:30
 */
public class KafkaMessage {

    private String topic;

    private String key;

    private List<String> messages;

    public KafkaMessage(String topic, String key, String message) {
        this.topic = topic;
        this.key = key;
        this.messages = new LinkedList<String>();
        this.messages.add(message);
    }

    public KafkaMessage(String topic, String key, List<String> messages) {
        this.topic = topic;
        this.key = key;
        this.messages = messages;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public List<String> getMessages() {
        return messages;
    }

    public void setMessages(List<String> messages) {
        this.messages = messages;
    }

    @Override
    public String toString() {
        return "KafkaMessage [topic=" + topic + ",key=" + key + ",messages=" + messages + "]";
    }

}
