package com.sachin.kafka.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 对数据进行持久化的抽象类
 * @author shicheng.zhang
 * @since 17-8-24 下午8:28
 */
public abstract class Journal<I> {

    // logger
    protected static final Logger logger = LoggerFactory.getLogger(Journal.class);

    /**
     * Queue for pending input
     */
    protected BlockingQueue<I> inputQueue;

    /**
     * Read Write Lock used to avoid the read and write conflict
     */
    private ReadWriteLock lock;

    /**
     * 内部工作线程
     */
    private Thread internalWorker;

    /**
     * 构造函数 对inputQueue、lock 、pros初始化
     */
    public Journal() {
        this.inputQueue = new LinkedBlockingQueue<I>();
        this.lock = new ReentrantReadWriteLock();

    }

    /**
     * 向队列中添加需要持久化的值
     *
     * @param val
     * @throws InterruptedException
     */
    public void put(I val) throws InterruptedException {
        this.inputQueue.put(val);
    }

    /**
     * 清空队列
     *
     * @throws InterruptedException
     */
    public void clean() {
        try {
            if (this.lock.writeLock().tryLock(1, TimeUnit.SECONDS)) {
                try {
                    this.inputQueue.clear();
                } finally {
                    this.lock.writeLock().unlock();
                }
            } else {
                logger.error("cannot clear the queue because of cannot get writelock !");
            }
        } catch (InterruptedException e) {
            logger.error("Occurs exception when clean the queue:" + e.toString());
        }
    }

    /**
     * 启动执行线程
     */
    public synchronized void start() {
        if (this.internalWorker != null) {
            logger.error("Journal already running");
            return;
        }
        this.internalWorker = new Thread(new Worker());
        this.internalWorker.start();
    }

    /**
     * 对给定数据进行处理
     *
     * @param val
     */
    protected abstract void process(I val);

    /**
     * 初始化相应的处理类
     */
    protected abstract void init();

    protected abstract void close();

    /**
     * 初始化处理类,不停地从inputQueue获取数据，并处理.
     */
    private void execute() {
        init();
        try {
            while (true) {
                process(inputQueue.take());
            }
        } catch (Throwable e) {
            logger.error("Journal Worker process file. ", e);
        } finally {
            this.close();
        }
    }

    /*****************************************************************************************************************************/
    /**
     * 内部工作线程
     */
    private class Worker implements Runnable {

        @Override
        public void run() {
            try {
                logger.info(this.getClass().getName() + " start!");
                execute();
            } catch (Throwable e) {
                logger.error("Journal Worker execute fail. ", e);
            }
        }
    }
}
