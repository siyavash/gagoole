import com.squareup.okhttp.Call;

public class TimeoutThread extends Thread
{
    private Call call;

    @Override
    public void run()
    {
        try
        {
            Thread.sleep(1500);
            if (!call.isCanceled())
            {
//                System.out.println("inside cancel");
                call.cancel();
            }

        } catch (InterruptedException e)
        {

        }
    }


    public void cancel()
    {
        interrupt();
    }

    public Call getCall()
    {
        return call;
    }

    public void setCall(Call call)
    {
        this.call = call;
    }
}
