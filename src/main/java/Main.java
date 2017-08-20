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

    public static void main(String[] args) throws Exception {
        loadProperties();
        Logger.start();

        if (initialMode)
            Seeds.publish();

        KafkaSubscribe kafkaSubscribe = new KafkaSubscribe();
        kafkaSubscribe.start();

        Crawler crawler = new Crawler(kafkaSubscribe);
        crawler.setThreads(NTHREADS);
        crawler.start();
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