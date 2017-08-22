package queue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

public class DistributedQueue extends Thread implements URLQueue {
    private Producer<String, String> producer;
    private String topicName;
    private final ArrayBlockingQueue<String> arrayBlockingQueue = new ArrayBlockingQueue<String>(1000000);
    private Properties publishProps = new Properties();
    private Properties consumeProps = new Properties();
    private final String groupId = "url-consumer";

    public DistributedQueue(String bootstrapServers, String topicName) {
        this.topicName = topicName;
        //below is for publish
        publishProps.put("bootstrap.servers", bootstrapServers);
        publishProps.put("acks", "all");
        publishProps.put("retries", 0);
        publishProps.put("batch.size", 16384);
        publishProps.put("linger.ms", 1);
        publishProps.put("buffer.memory", 33554432);
        publishProps.put("key.serializer", StringSerializer.class.getName());
        publishProps.put("value.serializer", StringSerializer.class.getName());

        producer = new KafkaProducer<String, String>(publishProps);

        //below is for subscribe
        consumeProps.put("bootstrap.servers", bootstrapServers);
        consumeProps.put("group.id", groupId);
        consumeProps.put("enable.auto.commit", "true");
        consumeProps.put("session.timeout.ms", "30000");
        consumeProps.put("key.deserializer", StringDeserializer.class.getName());
        consumeProps.put("value.deserializer", StringDeserializer.class.getName());
        consumeProps.put("auto.offset.reset", "earliest");
    }

    public void startThread() {
        this.start();
    }

    public String pop() throws InterruptedException{
        return arrayBlockingQueue.take();
    }

    public void push(ArrayList<String> arrayURLs) {
        for (String URL : arrayURLs)
            push(URL);
        producer.close();
    }

    public void push(String URL) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topicName, URL);
        producer.send(producerRecord);
    }

    @Override
    public int size() {
        return arrayBlockingQueue.size();
    }

    @Override
    public void run() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumeProps);
        consumer.subscribe(Arrays.asList(topicName));
        while (!isInterrupted()) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                try {
                    arrayBlockingQueue.put(record.value());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
//                System.out.println(record.value());
            }

        }
        consumer.close();
    }
}