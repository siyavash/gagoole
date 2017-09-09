import com.squareup.okhttp.Call;
import javafx.util.Pair;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import util.Profiler;

import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

public class TimeoutThread extends Thread
{
//    private LinkedBlockingQueue<Pair<Call, Long>> linkedBlockingQueue = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<Pair<HttpGet, Long>> linkedBlockingQueue = new LinkedBlockingQueue<>();

    @Override
    public void run()
    {
//        Pair<Call, Long> callPair;
//        Call call = null;
//        long timeDifference = 0;
//        while(true) {
//            try {
////                Profiler.setLinkedSize(linkedBlockingQueue.size());
//                callPair = linkedBlockingQueue.take();
//                if (callPair != null) {
//                    call = callPair.getKey();
//                    timeDifference = System.currentTimeMillis() - callPair.getValue();
//                }
//                if (timeDifference <= 5000){
//                    Thread.sleep(5000 - timeDifference);
//                }
//                if (call != null && !call.isCanceled()) {
//                    call.cancel();
//                }
//
//            } catch (InterruptedException ignored) {
//
//            }
//        }
        Pair<HttpGet, Long> futurePair;
        HttpGet get = null;
        long timeDifference = 0;

        while (!isInterrupted())
        {
            try
            {
                futurePair = linkedBlockingQueue.take();
                if (futurePair == null)
                {
                    continue;
                }

                get = futurePair.getKey();
                timeDifference = System.currentTimeMillis() - futurePair.getValue();

                if (timeDifference <= 20000)
                {
                    Thread.sleep(20000 - timeDifference);
                }

                if (get != null)
                {
                    get.releaseConnection();
                }

            } catch (InterruptedException e)
            {
                break;
            }
        }
    }

    public void addResponse(HttpGet get, Long time)
    {
        try
        {
            this.linkedBlockingQueue.put(new Pair<>(get, time));
        } catch (InterruptedException ignored)
        {

        }
    }


//    void addCall(Call call, Long time)
//    {
//        try {
//            this.linkedBlockingQueue.put(new Pair<>(call, time));
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//            //TODO: catch deciding
//        }
//    }
}
