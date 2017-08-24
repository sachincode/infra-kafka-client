package com.sachin.kafka.producer;

import kafka.producer.Partitioner;

import java.util.Random;

/**
 * @author shicheng.zhang
 * @since 17-8-24 下午8:42
 */
public class RandomPartitioner implements Partitioner {

    private Random random = new Random();

    @Override
    public int partition(Object key, int numPartitions) {
        return random.nextInt(numPartitions);
    }
}
