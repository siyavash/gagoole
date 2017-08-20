import javafx.util.Pair;
import org.jsoup.nodes.Attribute;
import org.jsoup.nodes.Attributes;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.ArrayList;

public class URLData
{
    private String url = ""; //TODO shorten the url
    private String passage = "";
    private String meta = "";
    private String title = "";
    private String insideLinks = "";

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

    public void setMeta(Elements allMetas)
    {
        StringBuilder stringBuilder = new StringBuilder();

        for (Element meta : allMetas)
        {
            Attributes metaAttributes = meta.attributes();
            buildMetaAttributesString(metaAttributes, stringBuilder);
        }

        meta = stringBuilder.toString();
    }

    private void buildMetaAttributesString(Attributes metaAttributes, StringBuilder stringBuilder)
    {
        for (Attribute metaAttribute : metaAttributes)
        {
            stringBuilder.append(metaAttribute.getKey());
            stringBuilder.append(" , ");
            stringBuilder.append(metaAttribute.getValue());
            stringBuilder.append("\n");
        }
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

    public void setInsideLinks(ArrayList<Pair<String, String>> subLinks) //TODO better design?
    {
        StringBuilder stringBuilder = new StringBuilder();

        for (Pair<String, String> subLink : subLinks)
        {
            String linkName = "";
            if (subLink.getKey() != null)
            {
                linkName = subLink.getKey();
            }

            String anchorName = "";
            if (subLink.getValue() != null)
            {
                anchorName = subLink.getValue();
            }


            stringBuilder.append(linkName);
            stringBuilder.append(" , ");
            stringBuilder.append(anchorName);
            stringBuilder.append("\n");
        }

        insideLinks = stringBuilder.toString();
    }
}
