package com.sachin.kafka.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * 自定义的调度器，定时调度任务
 * 
 * @author shicheng.zhang
 * @since 17-8-24 下午8:38
 */
public class InfraKafkaScheduler {

    private static final Logger logger = LoggerFactory.getLogger(InfraKafkaScheduler.class);

    // 单例
    private static InfraKafkaScheduler instance;

    // 维护一个调度器列表
    private HashMap<String, SchedulerKernel> data = new HashMap<String, SchedulerKernel>();

    // private构造函数
    private InfraKafkaScheduler() {
        // singleton
    }

    /**
     * 获取instance
     *
     * @return
     */
    public static InfraKafkaScheduler getInstance() {

        if (instance == null) {
            synchronized (InfraKafkaScheduler.class) {
                if (instance == null) {
                    instance = new InfraKafkaScheduler();
                }
            }
        }

        return instance;
    }

    public InfraKafkaScheduler add(Runnable runnable, int threadNumber, long schedulerTime) {
        if (!data.containsKey(runnable.getClass().toString())) {
            SchedulerKernel sample = new SchedulerKernel();
            sample.scheduleAtFixedRate(runnable, threadNumber, schedulerTime);
            data.put(runnable.getClass().toString(), sample);
            logger.info("Runnable " + runnable.toString() + " is added");
        } else {
            logger.info("Runnable " + runnable.toString() + " has been added, ignore");
        }

        return this;
    }

    public void shutdown(Runnable runnable) {
        if (data.containsKey(runnable.getClass().toString())) {
            data.get(runnable.getClass().toString()).shutdown();
        } else {
            logger.warn(runnable.getClass().toString() + " doesn't exist, ignore shutdown");
        }
    }

    /**
     * 内置的调度器,通过ScheduledExeecutorService实现
     */
    class SchedulerKernel {

        private ScheduledExecutorService kafkaScheduler;

        public void scheduleAtFixedRate(Runnable runnable, int threadNumber, long schedulerTime) {
            if (kafkaScheduler == null) {
                synchronized (InfraKafkaScheduler.class) {
                    kafkaScheduler = Executors.newScheduledThreadPool(threadNumber, new ThreadFactory() {

                        @Override
                        public Thread newThread(Runnable runnable) {
                            Thread thread = new Thread(runnable, runnable.toString());
                            thread.setDaemon(true);
                            return thread;
                        }
                    });
                }
                kafkaScheduler.scheduleAtFixedRate(runnable, schedulerTime, schedulerTime, TimeUnit.SECONDS);
            }
        }

        public void shutdown() {
            logger.info(kafkaScheduler.toString() + " begin to shutdown");
            kafkaScheduler.shutdownNow();
        }
    }
}
