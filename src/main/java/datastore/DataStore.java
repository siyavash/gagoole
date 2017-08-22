package datastore;

import java.io.IOException;

/**
 * Created by Amir on 8/21/2017 AD.
 */
public interface DataStore {

    boolean exists(String url) throws IOException;

    void put(PageInfo pageInfo) throws IOException;

}
