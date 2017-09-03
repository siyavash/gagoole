import util.Profiler;
public class Gagoole {
    public static void main(String[] args) {
        Profiler.start();

        Crawler crawler = new Crawler();
        crawler.start();
    }
}
