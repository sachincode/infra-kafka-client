package com.sachin.kafka.log;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sachin.kafka.utils.KafkaConfigLoader;
import com.sachin.kafka.utils.KafkaConstants;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 如果kafka发送数据时失败，将这些信息持久化到本地log
 * 
 * @author shicheng.zhang
 * @since 17-8-24 下午8:29
 */
public class KafkaFailJournal extends Journal<KafkaMessage> {

    //
    private boolean journalEnable = false;

    // 用于保存失败message的本地文件
    private String journalFile = "logs/failed-messages.log";

    // writer
    private BufferedWriter writer;

    // counter
    private AtomicLong num = new AtomicLong();

    // singleton
    public static KafkaFailJournal journal = null;

    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * 获取实例 singleton
     *
     * @return
     */
    public static KafkaFailJournal getInstance() {

        if (journal == null) {
            synchronized (KafkaFailJournal.class) {
                if (journal == null) {
                    journal = new KafkaFailJournal();
                }
            }
        }
        return journal;
    }

    private KafkaFailJournal() {
        super();
        // 获取properties
        Properties props = KafkaConfigLoader.loadPropertyFile(
                this.getClass().getClassLoader().getResourceAsStream(KafkaConstants.KAFKA_CONFIG_FILE));
        // 是否开启journal功能
        journalEnable = props.getProperty(KafkaConstants.SEND_FAIL_MESSAGE_RECORD_ENABLE, "false")
                .equalsIgnoreCase("true");
        // 如果开启,则初始化writer
        if (journalEnable) {
            journalFile = props.getProperty(KafkaConstants.SEND_FAIL_MESSAGE_RECORD_FILE,
                    System.getProperty("user.dir") + "/" + journalFile);
            try {
                writer = new BufferedWriter(new FileWriter(journalFile, true));
            } catch (IOException e) {
                logger.error("open " + journalFile + "error.", e);
            }
        }
    }

    @Override
    public void put(KafkaMessage val) throws InterruptedException {
        if (this.journalEnable) {
            this.inputQueue.put(val);
        }
    }

    @Override
    protected void process(KafkaMessage val) {
        if (!journalEnable) {
            logger.warn("send.fail.message.record.enable is false , do not record failed message:" + val.toString());
            return;
        }

        num.incrementAndGet();

        String message = null;
        if (val != null) {
            try {
                message = mapper.writeValueAsString(val);
            } catch (JsonProcessingException e) {
                logger.error("write value to json error.", e);
                return;
            }
        } else {
            return;
        }

        try {
            writer.write(message);
            writer.newLine();
            writer.flush();
        } catch (FileNotFoundException e0) {
            try {
                writer = new BufferedWriter(new FileWriter(journalFile, true));
            } catch (IOException e1) {
                logger.error("open " + journalFile + "error.", e1);
            }
        } catch (IOException e2) {
            logger.error("writer " + journalFile + " error.", e2);
        }
    }

    @Override
    protected void init() {
    }

    @Override
    protected void close() {
        try {
            writer.close();
        } catch (Throwable e) {
            logger.error("flush " + journalFile + " error.", e);
        }
    }

    /**
     * 是否开启了record 失败message功能
     *
     * @return
     */
    public boolean isRecordEnable() {
        return this.journalEnable;
    }

}
