package com.pof.dataops.kafka.consumer;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by glo on 12/15/2016.
 */
public class KafkaConsumerManualOffsetApp {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "10.100.70.133:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "firstGroup" );
        props.put("enable.auto.commit", "false");
        props.put("max.partition.fetch.bytes", "2097152");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        ArrayList<String> topics = new ArrayList<String>();
        topics.add("test");
        consumer.subscribe(topics);

        final int batchSize = 10;

        try {
            while(true) {

                ConsumerRecords<String, String> records = consumer.poll(100);

                int countSize = 0;


                for (ConsumerRecord<String, String> record : records) {

                    System.out.println(
                            String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s", record.topic(), record.partition(), record.offset(), record.key(), record.value())
                    );
                    countSize++;

                    if (countSize > batchSize) {
                        countSize = 0;
                        System.out.println("Committing changes...");
                        consumer.commitSync();
                    }
                }

            }
        }
        catch(CommitFailedException e) {
            System.out.println("Unexpected commit exception"  + e.getMessage());
        }
        catch (Throwable e) {
            System.out.println(e.getMessage());
        } finally {
            consumer.close();
        }
    }

}
