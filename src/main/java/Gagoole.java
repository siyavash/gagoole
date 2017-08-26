import util.LogStatus;

public class Gagoole {
    public static void main(String[] args) {
        LogStatus.start();

        Crawler crawler = new Crawler();
        crawler.start();
    }
}
