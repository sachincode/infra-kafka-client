package com.sachin.kafka.consumer;

import com.sachin.kafka.exception.InfraKafkaException;
import com.sachin.kafka.exception.InfraKafkaExceptionMonitor;
import com.sachin.kafka.utils.KafkaConfigLoader;
import com.sachin.kafka.utils.KafkaConstants;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author shicheng.zhang
 * @since 17-8-24 下午8:40
 */

public class InfraKafkaConsumer {

    private static final Logger logger = LoggerFactory.getLogger(InfraKafkaConsumer.class);

    // kafka exception monitor
    private static InfraKafkaExceptionMonitor kafkaMonitor;

    // 初始化kafka exception monitor
    static {
        kafkaMonitor = new InfraKafkaExceptionMonitor();
        kafkaMonitor.start(120, 60, 100, "Kafka consumer alarm");
    }

    // kafka topic
    private final String topic;

    // kafka consumer group
    private final String group;

    // kafka inner consumer
    InternalConsumer internalConsumer;

    // 已经消费的message条数
    private long count = 0;

    // commit per batch size
    private int commitSize = 1000;

    // commit manual
    private boolean commitManual = false;

    private ConsumerIterator<byte[], byte[]> it;

    public InfraKafkaConsumer(String topic, String group) {
        this(topic, group, 1000, false);
    }

    /**
     *
     * @param topic
     * @param group
     * @param commitSize if value -1 ,need commit offset manual
     */
    public InfraKafkaConsumer(String topic, String group, int commitSize, boolean commitManual) {
        this.topic = topic;
        this.group = group;
        this.it = reconnect();
        this.commitSize = commitSize;
        this.commitManual = commitManual;
    }

    /**
     * 从kafka读取一条message，将内容以String类型返回给调用者
     *
     * @return
     */
    public String getMessage() {
        String result;
        // 消费一条数据,结果保存在result里
        while (true) {
            try {
                if (it.hasNext()) {
                    MessageAndMetadata<byte[], byte[]> msgAndMeta = it.next();
                    try {
                        result = new String(msgAndMeta.message(), KafkaConstants.KAFKA_DEFAULT_ENCODING);
                        // increasing counting
                        count++;
                        // log
                        if (count % 100000 == 0) {
                            logger.info("Thread id +" + Thread.currentThread().getId() + "has been consumed " + count
                                    + " tips data");
                        }
                        // commit for every 1000 requests
                        // 除了按消费条数进行commit之外,kafka默认每10秒也会做一次commit
                        if (!this.commitManual) {
                            if (count % this.commitSize == 0) {
                                commitOffset();
                            }
                        }
                    } catch (UnsupportedEncodingException e) {
                        logger.error(e.toString());
                        // 遇到UnsupportedEncodingException异常时,重新读
                        continue;
                    }
                    break;
                } else {
                    logger.info("there is no data in kafka");
                }
            } catch (Throwable e) {
                //
                InfraKafkaException exception = new InfraKafkaException(
                        "An Error occurs during fetching message from kafka");
                //
                logger.error(exception.toString());
                //
                monitorException(exception);
                // try again until the connection is ok
                it = reconnect();
            }
        }

        return result;
    }

    public boolean commitOffset() {
        this.internalConsumer.commitOffsets();
        return true;

    }

    /**
     * 尝试和kafka server建立连接
     *
     * @return
     */
    private ConsumerIterator<byte[], byte[]> reconnect() {

        logger.info("consumer begins to connect");
        // reconnect之前，先关闭
        if (this.internalConsumer != null) {
            this.internalConsumer.close();
        }
        //
        boolean success = false;
        while (!success) {
            if (this.internalConsumer == null) {
                this.internalConsumer = new InternalConsumer(topic, group);
            } else {
                this.internalConsumer.close();
            }

            try {
                this.it = this.internalConsumer.initAndGetStreamIterator();
                success = true;
            } catch (InfraKafkaException e) {
                logger.error("consumer con not connect to kafka , wait for a while and retry", e);
                monitorException(e);
                try {
                    Thread.sleep(KafkaConstants.CONSUMER_RECONNECT_INTERVAL_MS);
                } catch (InterruptedException e1) {
                    // do nothing
                }
            }
        }
        //
        return this.it;
    }

    private void monitorException(InfraKafkaException e) {
        monitorException(e, "");

    }

    private void monitorException(InfraKafkaException e, String alarmString) {
        kafkaMonitor.recordException(e, alarmString);

    }

    /*************************************************************************************************/
    /**
     * InternalConsumer持有真正的kafka consumer
     */
    class InternalConsumer {

        // kafka topic
        private final String topic0;

        // kafka consumer's group
        private final String group0;

        // consumer connector
        private ConsumerConnector connector;

        // 迭代器 (可看作消费到的数据)
        private ConsumerIterator<byte[], byte[]> it0;

        public InternalConsumer(String topic, String group) {
            this.topic0 = topic;
            this.group0 = group;
        }

        /**
         * 初始化并获取 Stream Iterator
         *
         * @return
         * @throws InfraKafkaException
         */
        public ConsumerIterator<byte[], byte[]> initAndGetStreamIterator() throws InfraKafkaException {
            // 初始化kafka consumer
            initKafkaConsumer(group0);
            this.it0 = initKafkaStreams(topic0);
            return it0;
        }

        /**
         * 初始化kafka consumer
         * <p>
         * 也即初始化Consumer Connector
         *
         * @param group
         * @throws InfraKafkaException
         */
        private void initKafkaConsumer(String group) throws InfraKafkaException {
            // 初始化kafka consumer connector
            connector = kafka.consumer.Consumer.createJavaConsumerConnector(makeConnectorConfig(group));
        }

        /**
         * 从kafka consumer connector里初始化kafka stream
         *
         * @param topic
         * @return
         * @throws InfraKafkaException
         */
        private ConsumerIterator<byte[], byte[]> initKafkaStreams(String topic) throws InfraKafkaException {
            // 封装topicCountMap
            Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
            topicCountMap.put(topic, 1); // we use only 1 stream per connector

            // 根据topicCountMap
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap;
            try {
                consumerMap = connector.createMessageStreams(topicCountMap);
            } catch (kafka.common.InvalidMessageSizeException e) {
                throw new InfraKafkaException("the client has requested a range no longer available on the server!", e);
            } catch (kafka.common.NoBrokersForPartitionException e) {
                throw new InfraKafkaException("No brokers exists, please restart brokers!", e);
            } catch (kafka.common.OffsetOutOfRangeException e) {
                throw new InfraKafkaException(
                        "The client has requested a range no longer available on the server:offset out of range!", e);
            } catch (kafka.common.UnknownCodecException e) {
                throw new InfraKafkaException(
                        "The client has requested a range no longer available on the server:unknown code!", e);
            } catch (kafka.common.UnknownException e) {
                throw new InfraKafkaException("UnknownException!", e);
            } catch (kafka.common.UnknownMagicByteException e) {
                throw new InfraKafkaException(
                        "The client has requested a range no longer available on the server:unknown magic byte!", e);
            } catch (kafka.consumer.ConsumerTimeoutException e) {
                throw new InfraKafkaException("Time out,need restart the broker!", e);
            } catch (Throwable e) {
                throw new InfraKafkaException("Unknown exception", e);
            }

            return consumerMap.get(topic).get(0).iterator();
        }

        /**
         * 从配置文件里为指定的group加载配置信息
         *
         * @param group
         * @return
         * @throws InfraKafkaException
         */
        private ConsumerConfig makeConnectorConfig(String group) throws InfraKafkaException {
            Properties props = KafkaConfigLoader.loadPropertyFile(
                    this.getClass().getClassLoader().getResourceAsStream(KafkaConstants.KAFKA_CONFIG_FILE));

            // define the group id
            props.put("group.id", group);
            ConsumerConfig resultConfig;
            try {
                resultConfig = new ConsumerConfig(props);
                return resultConfig;

            } catch (kafka.common.InvalidConfigException e) {
                throw new InfraKafkaException("KafkaException:The given config parameter has invalid values!", e);
            } catch (kafka.common.UnavailableProducerException e) {
                throw new InfraKafkaException("KafkaException:The producer pool cannot be initialized!", e);
            } catch (Exception e) {
                throw new InfraKafkaException("KafkaException:Other exception!", e);
            }
        }

        /**
         * 关闭链接
         */
        public void close() {
            if (connector != null) {
                connector.shutdown();
            }
            connector = null;
            it0 = null;
        }

        /**
         * 提交connector连接的所有partitions的偏移量
         */
        public void commitOffsets() {
            if (connector != null) {
                connector.commitOffsets();
            }
        }
    }

}
