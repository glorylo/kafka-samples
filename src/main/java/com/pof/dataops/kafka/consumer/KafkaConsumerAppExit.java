package com.pof.dataops.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by glo on 12/20/2016.
 */
public class KafkaConsumerAppExit {
    public KafkaConsumer consumer;


    public static void main (String[] args) {

        KafkaConsumerAppExit consumerApp = new KafkaConsumerAppExit();

        consumerApp.Init();
        consumerApp.Subscribe();

        final Thread mainThread = Thread.currentThread();
        // Registering a shutdown hook so we can exit cleanly
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Starting exit...");
                // Note that shutdownhook runs in a separate thread, so the only thing we can safely do to a consumer is wake it up
                consumerApp.consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            while(true) {

                ConsumerRecords<String, String> records = consumerApp.consumer.poll(100);

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(
                            String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s", record.topic(), record.partition(), record.offset(), record.key(), record.value())
                    );
                }

            }
        }
        catch (WakeupException e) {
            // ignore for shutdown
        }

        catch (Throwable e) {
            System.out.println(e.getMessage());
        } finally {
            consumerApp.consumer.close();
        }


    }

    public void Init() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.100.70.133:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "firstGroup" );
        props.put("enable.auto.commit", "true");
        props.put("max.partition.fetch.bytes", "2097152");
        consumer = new KafkaConsumer<String, String>(props);
    }

    public void Subscribe() {
        ArrayList<String> topics = new ArrayList<String>();
        topics.add("test");
        consumer.subscribe(topics);
    }

}
