import com.squareup.okhttp.Call;
import javafx.util.Pair;

import java.util.concurrent.LinkedBlockingQueue;

public class TimeoutThread extends Thread
{
    private Call call = null;
    private LinkedBlockingQueue<Pair<Call, Long>> linkedBlockingQueue = new LinkedBlockingQueue<>();

    @Override
    public void run()
    {
        Pair<Call, Long> callPair;
        Call call = null;
        long timeDifference;
        while(true) {
            callPair = null;
            try {
                System.out.println("1");
                callPair = linkedBlockingQueue.take();
                System.out.println("2");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (callPair != null) {
                System.out.println("3");
                call = callPair.getKey();
                System.out.println("4");
            }
            timeDifference = System.currentTimeMillis() - callPair.getValue();
            try {
                if (timeDifference <= 1500){
                    System.out.println("5");
                    Thread.sleep(1500 - timeDifference);
                    System.out.println("6");
                }
                if (call != null && !call.isCanceled()) {
                    System.out.println("7");
                    call.cancel();
                    System.out.println("8");
                }
            } catch (InterruptedException e) {

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
