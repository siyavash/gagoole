package queue;

import java.util.ArrayList;

/**
 * Created by Amir on 8/21/2017 AD.
 */
public interface URLQueue {

    String pop() throws InterruptedException;

    void push(ArrayList<String> arrayURLs);

    void push(String URL);
}
