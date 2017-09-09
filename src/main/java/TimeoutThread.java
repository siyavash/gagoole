import com.squareup.okhttp.Call;
import javafx.util.Pair;
import org.apache.http.HttpResponse;
import util.Profiler;

import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

public class TimeoutThread extends Thread
{
//    private LinkedBlockingQueue<Pair<Call, Long>> linkedBlockingQueue = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<Pair<Future<HttpResponse>, Long>> linkedBlockingQueue = new LinkedBlockingQueue<>();

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
        Pair<Future<HttpResponse>, Long> futurePair;
        Future<HttpResponse> futureResponse = null;
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

                futureResponse = futurePair.getKey();
                timeDifference = System.currentTimeMillis() - futurePair.getValue();

                if (timeDifference <= 5000)
                {
                    Thread.sleep(5000 - timeDifference);
                }

                if (futureResponse != null && !futureResponse.isCancelled())
                {
                    futureResponse.cancel(true);
                }

            } catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
    }

    public void addResponse(Future<HttpResponse> futureResponse, Long time)
    {
        try
        {
            this.linkedBlockingQueue.put(new Pair<>(futureResponse, time));
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
