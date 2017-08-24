package com.sachin.kafka.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 用于监控Kafka的异常情况,将异常信息定期展示(log或邮件形式)
 * @author shicheng.zhang
 * @since 17-8-24 下午8:38
 */
public class InfraKafkaExceptionMonitor {

    private static final Logger logger = LoggerFactory.getLogger(InfraKafkaExceptionMonitor.class);

    private AtomicInteger exceptionCount = new AtomicInteger(0);

    private boolean suspend = false;

    /**
     * 重连时间
     */
    private static int reConnectionTime;

    /**
     * 异常个数重置时间
     */
    private static int cleanExceptionTime;

    /**
     * 异常名称
     */
    private static String exceptionName;

    /**
     * 异常总数
     */
    private static int totalException;

    public void start(int reConnectionTime, int cleanExceptionTime, int totalException, String exceptionName) {
        InfraKafkaExceptionMonitor.reConnectionTime = reConnectionTime;
        InfraKafkaExceptionMonitor.cleanExceptionTime = cleanExceptionTime;
        InfraKafkaExceptionMonitor.exceptionName = exceptionName;
        InfraKafkaExceptionMonitor.totalException = totalException;
        InfraKafkaScheduler.getInstance().add(new CleanException(), 1, InfraKafkaExceptionMonitor.cleanExceptionTime)
                .add(new ReConnection(), 1, InfraKafkaExceptionMonitor.reConnectionTime);
    }

    public boolean isSuspend() {
        return suspend;
    }

    /**
     * Description: get exception to handler
     *
     * @param e
     * @param alarmString
     */
    public void recordException(InfraKafkaException e, String alarmString) {
        if (exceptionCount.incrementAndGet() > totalException) {
            logger.error(String.format("MQ send error:%s, currentErrorCount=%s, errorCountMax=%s ", e.getMessage(),
                    exceptionCount.get(), totalException), e);
            suspend = true;

            // TODO: send alarm
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String content = String.format("Time: %s MQ send error:%s, currentErrorCount=%s, errorCountMax=%s ",
                    sdf.format(new Date(System.currentTimeMillis())), e.getMessage(), exceptionCount.get(),
                    totalException);
            // 记录这段时间的异常情况
            // 这块可以通过邮件的方式发出报警信息.//todo
            //
            logger.warn(content);
            // 将exception重置为0
            exceptionCount.set(0);
        }
    }

    class ReConnection implements Runnable {

        @Override
        public void run() {
            logger.info(exceptionName + " reconnect -----------------------");
            suspend = false;
        }
    }

    class CleanException implements Runnable {

        @Override
        public void run() {
            logger.info(exceptionName + ": exceptionCount=" + exceptionCount.intValue()
                    + " clean up exception numbers! -----------------------");
            exceptionCount.set(0);
        }

    }

}
