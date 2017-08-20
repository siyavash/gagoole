import com.google.common.net.InternetDomainName;

import java.net.MalformedURLException;
import java.net.URL;

public class DomainHandler {

    private String stringUrl;

    public DomainHandler(String url) {
        stringUrl = url;
    }

    public String getDomain() {
        try {
            URL url = new URL(stringUrl);
            String hostName = url.getHost();
            return InternetDomainName.from(hostName).topPrivateDomain().toString();
        } catch (MalformedURLException e) {
            System.err.println("malformed url exception: " + stringUrl);
            return null;
        } catch (IllegalArgumentException ex) {
            System.err.println("Illegal argument exception: " + stringUrl);
            return null;
        } catch (IllegalStateException ex) {
            System.err.println("Illegal state exception: " + stringUrl);
            return null;
        }
    }
}
