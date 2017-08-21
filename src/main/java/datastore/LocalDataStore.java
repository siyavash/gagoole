package datastore;

import java.io.IOException;
import java.util.HashSet;

/**
 * Created by Amir on 8/21/2017 AD.
 */
public class LocalDataStore implements DataStore {

    private HashSet<String> store = new HashSet<String>();

    @Override
    public boolean exists(String url) throws IOException {
        return store.contains(url);
    }

    @Override
    public void put(PageInfo pageInfo) throws IOException {

    }

    public void put(String url) {
        store.add(url);
    }
}
