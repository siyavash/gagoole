package datastore;

import java.io.IOException;

/**
 * Created by Amir on 8/21/2017 AD.
 */
public interface DataStore {

    public boolean exists(String url) throws IOException;

    public void put(PageInfo pageInfo) throws IOException;

}
