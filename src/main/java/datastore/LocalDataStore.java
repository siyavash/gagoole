package datastore;

import java.io.IOException;
import java.util.HashSet;

/**
 * Created by Amir on 8/21/2017 AD.
 */
public class LocalDataStore implements DataStore {

    private HashSet<PageInfo> store = new HashSet<PageInfo>();

    @Override
    public boolean exists(String url) throws IOException {
        return false;
    }

    @Override
    public void put(PageInfo pageInfo) throws IOException {

    }
}
