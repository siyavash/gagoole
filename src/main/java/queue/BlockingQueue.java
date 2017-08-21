package queue;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by Amir on 8/21/2017 AD.
 */
public class BlockingQueue implements URLQueue {

    private ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<String>(1000 * 1000);

    @Override
    public String pop() throws InterruptedException {
        return queue.take();
    }

    @Override
    public void push(ArrayList<String> arrayURLs) {
        if (arrayURLs != null)
            queue.addAll(arrayURLs);
    }

    @Override
    public void push(String URL) {
        if (URL != null)
            queue.add(URL);
    }
}
