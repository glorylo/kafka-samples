/**
 * Created by glo on 12/14/2016.
 */
package com.pof.dataops.kafka.producer;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
public class KafkaProducerApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.100.70.133:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        try {

            for (int i = 0; i < 100; i++) {
                String message = "this is a test producer record: " + Integer.toString(i);
                ProducerRecord record = new ProducerRecord("test", message);
                System.out.println(message);
                producer.send(record);
            }
        } catch (Throwable throwable) {
            //System.out.printf("%s", throwable.getStackTrace());
            throwable.printStackTrace();
        }
        finally {
            producer.close();
        }

    }

}
