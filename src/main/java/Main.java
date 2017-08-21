import Util.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Main {
    private static int NTHREADS;

    public static void main(String[] args) {
        Logger.start();

        Crawler crawler = new Crawler();
        crawler.start();
    }
}