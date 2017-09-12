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
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;

public class DistributedQueue extends Thread implements URLQueue
{
    private Producer<String, String> producer;
    private String topicName;
    private final ArrayBlockingQueue<String> arrayBlockingQueue = new ArrayBlockingQueue<>(5000);
    private Properties publishProps = new Properties();
    private Properties consumeProps = new Properties();

    private final String groupId = UUID.randomUUID().toString();

    public DistributedQueue(String bootstrapServers, String topicName)
    {
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

        producer = new KafkaProducer<>(publishProps);

        //below is for subscribe
        consumeProps.put("bootstrap.servers", bootstrapServers);
        consumeProps.put("group.id", groupId);
        consumeProps.put("enable.auto.commit", "true");
        consumeProps.put("session.timeout.ms", "30000");
        consumeProps.put("key.deserializer", StringDeserializer.class.getName());
        consumeProps.put("value.deserializer", StringDeserializer.class.getName());
        consumeProps.put("auto.offset.reset", "earliest");
    }

    public void startThread()
    {
        this.start();
    }

    public String pop() throws InterruptedException
    {
        return arrayBlockingQueue.take();
    }

    public void push(ArrayList<String> arrayURLs)
    {
        for (String URL : arrayURLs)
            push(URL);
    }

    public void push(String URL)
    {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, URL);
        producer.send(producerRecord);
    }

    @Override
    public int size()
    {
        return arrayBlockingQueue.size();
    }

    @Override
    public void close()
    {
        producer.close();
    }

    @Override
    public void run()
    {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumeProps);
        consumer.subscribe(Arrays.asList(topicName));
        while (!isInterrupted())
        {
            try
            {
                ConsumerRecords<String, String> records = consumer.poll(3000);
                for (ConsumerRecord<String, String> record : records)
                {
                    arrayBlockingQueue.put(record.value());
                }
            } catch (InterruptedException e)
            {
                break;
            }
        }
        consumer.close();
    }
}
