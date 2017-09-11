import util.Profiler;

public class Gagoole {
    public static void main(String[] args) {
        Profiler.start();

        Crawler crawler = new Crawler();
        crawler.start();

        if (args.length != 0)
        {
            new Thread(() -> {
                try
                {
                    Thread.sleep(Long.parseLong(args[0]) * 1000);
                    crawler.close();
                } catch (InterruptedException ignored)
                {

                }
            }).start();
        }
    }

}
