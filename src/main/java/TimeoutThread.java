import com.squareup.okhttp.Call;
import javafx.util.Pair;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import util.Profiler;

import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

public class TimeoutThread extends Thread
{
    private LinkedBlockingQueue<Pair<Call, Long>> linkedBlockingQueue = new LinkedBlockingQueue<>();

    @Override
    public void run()
    {
        Pair<Call, Long> callPair;
        Call call = null;
        long timeDifference = 0;
        while(true) {
            try {
                callPair = linkedBlockingQueue.take();
                if (callPair != null) {
                    call = callPair.getKey();
                    timeDifference = System.currentTimeMillis() - callPair.getValue();
                }
                if (timeDifference <= 5000){
                    Thread.sleep(5000 - timeDifference);
                }
                if (call != null && !call.isCanceled()) {
                    call.cancel();
                }

            } catch (InterruptedException e) {
                break;
            }
        }
    }


    void addCall(Call call, Long time)
    {
        try {
            this.linkedBlockingQueue.put(new Pair<>(call, time));
        } catch (InterruptedException e) {
            Profiler.error(e.getMessage());
        }
    }
}
