package datastore;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Amir on 8/21/2017 AD.
 */
public interface DataStore {

    boolean exists(String url) throws IOException;

    boolean[] exists(ArrayList<String> urls) throws IOException;

    void put(PageInfo pageInfo) throws IOException;

}
