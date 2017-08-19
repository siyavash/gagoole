import javafx.util.Pair;

import java.util.ArrayList;

public class URLData
{
    private String url;
    private String passage;
    private String meta;
    private String title;
    private String insideLinks;

    public String getUrl()
    {
        return url;
    }

    public void setUrl(String url)
    {
        this.url = url;
    }

    public String getPassage()
    {
        return passage;
    }

    public void setPassage(String passage)
    {
        this.passage = passage;
    }

    public String getMeta()
    {
        return meta;
    }

    public void setMeta(String meta)
    {
        this.meta = meta;
    }

    public String getTitle()
    {
        return title;
    }

    public void setTitle(String title)
    {
        this.title = title;
    }

    public String getInsideLinks()
    {
        return insideLinks;
    }

    public void setInsideLinks(ArrayList<Pair<String, String>> insideLinks)
    {
        for (Pair<String, String> insideLink : insideLinks)
        {
            this.insideLinks += insideLink.getKey() + " , " + insideLink.getValue() + "\n";
        }
    }
}
