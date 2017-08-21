import Util.Logger;

public class Gagoole {
    public static void main(String[] args) {
        Logger.start();

        Crawler crawler = new Crawler();
        crawler.start();
    }
}
