package queue;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;

public class LocalQueue implements URLQueue {

    private ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<String>(1000 * 1000);

    public String pop() throws InterruptedException {
        return queue.take();
    }

    public void push(ArrayList<String> arrayURLs) {
        for (String url : arrayURLs) {
            if (queue.size() < 900 * 1000)
                try {
                    queue.put(url);
                } catch (InterruptedException e) {
                    break;
                }
        }
    }

    public void push(String URL) {
        if (queue.size() < 900 * 1000)
            try {
                queue.put(URL);
            } catch (InterruptedException ignored) {

            }
    }

    public void startThread() {

    }

    @Override
    public int size() {
        return queue.size();
    }

    @Override
    public void close() {

    }
}
