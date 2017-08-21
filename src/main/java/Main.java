import Util.Logger;
import queue.Seeds;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Main {
    private static int NTHREADS;

    public static void main(String[] args) {
        loadProperties();
        Logger.start();

        if (initialMode) {
            Seeds.publish(); // TODO: change to func
            System.out.println("seeds are published to kafka");
            return;
        }

        Crawler crawler = new Crawler(kafkaSubscribe); // TODO: put properties in constructor
        crawler.setThreads(NTHREADS);
        crawler.start();
    }
}