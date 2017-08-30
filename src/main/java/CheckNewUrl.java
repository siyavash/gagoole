import datastore.DataStore;
import util.Profiler;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

public class CheckNewUrl extends Thread {

    private DataStore urlDatabase;
    private ArrayBlockingQueue<String> properUrls;
    private ArrayBlockingQueue<String> newUrls;

    public CheckNewUrl(DataStore urlDatabase, ArrayBlockingQueue<String> properUrls){
        this.urlDatabase = urlDatabase;
        this.properUrls = properUrls;
        newUrls = new ArrayBlockingQueue<>(100000);
    }

    private boolean checkIfAlreadyExist(String urlToVisit) {   //I/O work
        boolean exist = true;
        try {
            exist = urlDatabase.exists(urlToVisit);
        } catch (IOException e) {
            e.printStackTrace();
            //TODO: catch deciding
        }
        return exist;
    }

    @Override
    public void run() {
        while(true){
            long checkExistTime = System.currentTimeMillis();
            String urlToVisit = null;
            try {
                urlToVisit = properUrls.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
                //TODO: catch deciding
            }
            boolean isInDataStore = checkIfAlreadyExist(urlToVisit);
            checkExistTime = System.currentTimeMillis() - checkExistTime;
            Profiler.checkExistenceInDataStore(urlToVisit, checkExistTime, isInDataStore);
            if (isInDataStore)
                continue;
            try {
                newUrls.put(urlToVisit);
            } catch (InterruptedException e) {
                e.printStackTrace();
                //TODO: catch deciding
            }
        }

    }
}
