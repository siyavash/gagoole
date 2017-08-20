package kafka;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;


public class KafkaSubscribe extends Thread {
    private static Logger logger = Logger.getLogger(Class.class.getSimpleName());
    private final String topicName = "test";
    private final String bootstrapServer = "master:9092,slave:9092";
    private final String groupId = "test";
    private final ArrayBlockingQueue<String> urlsArrayBlockingQueue = new ArrayBlockingQueue<String>(1000000);

    @Override
    public void run() {
        Properties props = new Properties();
        logger.debug("SALAM");
        props.put("bootstrap.servers", bootstrapServer);
        props.put("group.id", groupId);

        props.put("enable.auto.commit", "false");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName));
        while (!isInterrupted()) {
            ConsumerRecords<String, String> records = consumer.poll(10000);
            for (ConsumerRecord<String, String> record : records) {
                try {
                    urlsArrayBlockingQueue.put(record.value());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println(record.value());
            }
            try {
                consumer.commitSync();
            } catch (CommitFailedException e) {
                //// TODO: 8/19/17 log.error("commit failed", e)
            }

        }
        consumer.close();
    }


    public ArrayBlockingQueue<String> getUrlsArrayBlockingQueue() {
        return urlsArrayBlockingQueue;
    }
}