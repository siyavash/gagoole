import kafka.KafkaSubscribe;

public class Main {
    public static void main(String[] args) throws Exception {
        KafkaSubscribe kafkaSubscribe = new KafkaSubscribe();
        kafkaSubscribe.start();
        Crawler crawler = new Crawler(kafkaSubscribe);
        crawler.setThreads(200);
        crawler.start();
    }
}