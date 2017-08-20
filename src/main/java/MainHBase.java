import java.io.IOException;

public class MainHBase {

    public static void main(String[] args) throws IOException {
        GagooleHBase hBase = new GagooleHBase("smallTable", "columnFamily");
        URLData urlData=new URLData();
        urlData.setMeta("META");
        urlData.setPassage("PASSAGE");
        urlData.setTitle("Title");
        urlData.setUrl("URL");

//        hBase.put(urlData);
    }
}
