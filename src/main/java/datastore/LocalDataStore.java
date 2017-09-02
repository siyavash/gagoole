package datastore;

import java.io.IOException;
import java.util.ArrayList;
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
    public boolean[] exists(ArrayList<String> urls) throws IOException
    {
        boolean[] result = new boolean[urls.size()];

        for (int i = 0; i < urls.size(); i++)
        {
            result[i] = store.contains(urls);
        }

        return result;
    }

    @Override
    public void put(PageInfo pageInfo) throws IOException {
        store.add(pageInfo.getUrl());
    }

}
