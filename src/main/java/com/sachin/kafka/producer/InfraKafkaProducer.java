package com.sachin.kafka.producer;

import com.sachin.kafka.exception.InfraKafkaException;
import com.sachin.kafka.exception.InfraKafkaExceptionMonitor;
import com.sachin.kafka.log.KafkaFailJournal;
import com.sachin.kafka.log.KafkaMessage;
import com.sachin.kafka.utils.KafkaConfigLoader;
import com.sachin.kafka.utils.KafkaConstants;
import com.sachin.kafka.utils.KafkaValidators;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @author shicheng.zhang
 * @since 17-8-24 下午8:42
 */
public class InfraKafkaProducer {

    // logger
    private static final Logger logger = LoggerFactory.getLogger(InfraKafkaProducer.class);

    // kafka exception monitor
    private static InfraKafkaExceptionMonitor kafkaMonitor;

    // 是否开启记录失败message功能 (也即是否开启journal)
    // private static boolean record_enable = false;

    // journal
    private static KafkaFailJournal journal;

    // instance
    private static InfraKafkaProducer instance = new InfraKafkaProducer();

    /**
     * 默认尝试次数
     */
    private static final int DEFAULT_RETRY_TIMES = 2;

    /**
     * 默认超时时间
     */
    private static final int DEFAULT_TIMEOUT = 3000;

    // producer
    private Producer<String, String> producer;

    // suspend flag
    private volatile boolean suspend;

    private InfraKafkaProducer() {
        // 初始化kafka exception monitor
        logger.info("setup kafka exception monitor!");
        kafkaMonitor = new InfraKafkaExceptionMonitor();
        kafkaMonitor.start(120, 60, 100, "Kafka producer alarm");

        // read configure
        Properties props = KafkaConfigLoader.loadPropertyFile(
                this.getClass().getClassLoader().getResourceAsStream(KafkaConstants.KAFKA_CONFIG_FILE));

        // 初始化journal
        journal = KafkaFailJournal.getInstance();
        if (journal.isRecordEnable()) {
            journal.start();
        }

        // 初始化producer
        try {
            producer = new Producer<String, String>(createProducerConfig(props));
        } catch (kafka.common.InvalidConfigException e) {
            monitorException(
                    new InfraKafkaException("KafkaException:The given config parameter has invalid values!", e), "");
        } catch (kafka.common.UnavailableProducerException e) {
            monitorException(new InfraKafkaException("KafkaException:The producer pool cannot be initialized !", e),
                    "");
        } catch (Exception e) {
            monitorException(new InfraKafkaException("KafkaException:Other exception!", e), "");
        }

    }

    /**
     * 获取kafka producer配置
     *
     * @return
     * @throws InfraKafkaException
     */
    private ProducerConfig createProducerConfig(Properties props) throws InfraKafkaException {
        ProducerConfig resultConfig = null;
        try {
            resultConfig = new ProducerConfig(props);
        } catch (kafka.common.InvalidConfigException e) {
            monitorException(
                    new InfraKafkaException("KafkaException:The given config parameter has invalid values!", e), "");
        } catch (kafka.common.UnavailableProducerException e) {
            monitorException(new InfraKafkaException("KafkaException:The producer pool cannot be initialized !", e),
                    "");
        } catch (Exception e) {
            monitorException(new InfraKafkaException("KafkaException:Other exception!", e), "");
        }
        return resultConfig;
    }

    private void monitorException(InfraKafkaException infraKafkaException, String alarmString) {
        kafkaMonitor.recordException(infraKafkaException, alarmString);
    }

    public static InfraKafkaProducer getInstance() {
        return instance;
    }

    /**
     * 发送一条message
     *
     * @param topic
     * @param partitionKey
     * @param msgData
     * @return
     */
    public boolean sendMessage(String topic, String partitionKey, String msgData) {
        return sendMessage(topic, partitionKey, Arrays.asList(msgData));
    }

    /**
     * 发送多条message
     *
     * @param topic
     * @param partitionKey
     * @param msgDataList
     * @return
     */
    public boolean sendMessage(String topic, String partitionKey, List<String> msgDataList) {
        List<KeyedMessage<String, String>> kms = new ArrayList<KeyedMessage<String, String>>(msgDataList.size());

        // construct keyedMessageList
        try {
            KafkaValidators.checkTopic(topic);
            for (String msgData : msgDataList) {
                KafkaValidators.checkMsgData(msgData);
                kms.add(new KeyedMessage<String, String>(topic, partitionKey, msgData));
            }
        } catch (InfraKafkaException e) {
            logger.error("Message is invalid :", e);
            return false;
        }

        // send
        final long beginTimestamp = System.currentTimeMillis();
        long endTimestamp = beginTimestamp;
        for (int times = 0; times <= DEFAULT_RETRY_TIMES
                && (endTimestamp - beginTimestamp) < DEFAULT_TIMEOUT; times++) {
            if (suspend) {
                // 如果infra producer被挂起,则不发送信息，直接返回false
                logger.error("Send message failed, producer is suspended");
                return false;
            }
            try {
                producer.send(kms);
                endTimestamp = System.currentTimeMillis();
                if (logger.isDebugEnabled()) {
                    logger.debug("Send Message Success:topic=" + topic + ", key=" + partitionKey + ", messages="
                            + msgDataList);
                }
                return true;
            } catch (kafka.common.NoBrokersForPartitionException e) {
                endTimestamp = System.currentTimeMillis();
                String errorMsg = "KafkaException:No brokers exists, please restart brokers!";
                monitorException(new InfraKafkaException(errorMsg, e), "");
                logger.error(errorMsg, e);
                times++;
                continue;
            } catch (kafka.common.QueueFullException e) {
                endTimestamp = System.currentTimeMillis();
                String errorMsg = "KafkaException:Async queue full,please reset parameters!";
                logger.error(errorMsg, e);
                monitorException(new InfraKafkaException(errorMsg, e), "");
                times++;
                continue;
            } catch (kafka.producer.ProducerClosedException e) {
                endTimestamp = System.currentTimeMillis();
                String errorMsg = "KafkaException:The producer is close!";
                logger.error(errorMsg, e);
                monitorException(new InfraKafkaException(errorMsg, e), "");
                times++;
                continue;
            } catch (kafka.producer.async.IllegalQueueStateException e) {
                endTimestamp = System.currentTimeMillis();
                String errorMsg = "KafkaException:The given config parameter has invalid value!";
                logger.error(errorMsg, e);
                monitorException(new InfraKafkaException(errorMsg, e), "");
                times++;
                continue;
            } catch (kafka.producer.async.MissingConfigException e) {
                endTimestamp = System.currentTimeMillis();
                String errorMsg = "KafkaException:Missing config parameters!";
                logger.error(errorMsg, e);
                monitorException(new InfraKafkaException(errorMsg, e), "");
                times++;
                continue;
            } catch (Exception e) {
                endTimestamp = System.currentTimeMillis();
                String errorMsg = "KafkaException:Send Message exception!";
                logger.error(errorMsg, e);
                monitorException(new InfraKafkaException(errorMsg, e), "");
                times++;
                continue;
            }
        }

        // 通过journal将发送失败的数据持久化到本地
        try {
            KafkaMessage kafkaMessage = new KafkaMessage(topic, partitionKey, msgDataList);
            journal.put(kafkaMessage);
        } catch (InterruptedException e) {
            logger.error("Occurs error when put kafkaMessage to fail file", e);
        }

        // 没发送成功,返回false
        return false;
    }
}
