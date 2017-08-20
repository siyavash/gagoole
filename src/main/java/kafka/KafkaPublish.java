package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Properties;

public class KafkaPublish {
    private Producer<String, String> producer;
    private final String bootstrapServers = "master:9092,slave:9092";

    private static KafkaPublish ourInstance = new KafkaPublish();

    public static KafkaPublish getInstance() {
        return ourInstance;
    }

    private KafkaPublish() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        producer = new KafkaProducer<>(props);

    }

    public void produceUrls(ArrayList<String> arrayURLs) {
        for (String URL : arrayURLs)
            produceUrl(URL);
    }

    public void produceUrl(String URL) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test", URL);
        producer.send(producerRecord);
    }

    public void stop() {
        producer.close();
    }

}