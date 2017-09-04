import com.squareup.okhttp.Call;
import javafx.util.Pair;

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
            callPair = null;
            try {

                callPair = linkedBlockingQueue.take();

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (callPair != null) {
                call = callPair.getKey();
                timeDifference = System.currentTimeMillis() - callPair.getValue();
            }
            try {
                if (timeDifference <= 1500){
                    Thread.sleep(1500 - timeDifference);
                }
                if (call != null && !call.isCanceled()) {
                    call.cancel();
                }
            } catch (InterruptedException e) {
                //TODO
            }
        }
    }


    public void addCall(Call call, Long time)
    {
        try {
            this.linkedBlockingQueue.put(new Pair<>(call, time));
        } catch (InterruptedException e) {
            e.printStackTrace();
            //TODO: catch deciding
        }
    }
}
