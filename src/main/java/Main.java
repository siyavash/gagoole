import Util.Logger;
import kafka.KafkaSubscribe;
import kafka.Seeds;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Main {
    private static int NTHREADS;
    private static boolean initialMode;

    public static void main(String[] args) {
        loadProperties();
        Logger.start();

        if (initialMode) {
            Seeds.publish(); // TODO: change to func
            System.out.println("seeds are published to kafka");
            return;
        }

        KafkaSubscribe kafkaSubscribe = new KafkaSubscribe();
        kafkaSubscribe.start();

        try {
            Crawler crawler = new Crawler(kafkaSubscribe); // TODO: put properties in constructor
            crawler.setThreads(NTHREADS);
            crawler.start();
        } catch (IOException e) {
            System.err.println("Error in connecting hbase:" + e);
            System.exit(20);
        }
    }

    private static void loadProperties() {
        Properties prop = new Properties();
        InputStream input = null;

        try {

            input = new FileInputStream("config.properties");
            prop.load(input);

            NTHREADS = Integer.parseInt(prop.getProperty("threads-number", "500"));
            initialMode = prop.getProperty("initial-mode", "true").equals("true");

        } catch (IOException ex) {
            System.err.println("error in reading config file:");
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}