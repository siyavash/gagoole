package kafka;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class Seeds {

    public static void publish() {
        try {
            ArrayList<String> seedUrls = new ArrayList<String>(500);
            File file = new File("seed.txt");
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                seedUrls.add("http://www." + line);
            }
            fileReader.close();
            KafkaPublish.getInstance().produceUrls(seedUrls);
        } catch (IOException e) {
            System.err.println("error in loading seed: " + e);
            System.exit(101);
        }
    }
}
