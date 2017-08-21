package datastore;

import javafx.util.Pair;
import org.jsoup.nodes.Attribute;
import org.jsoup.nodes.Attributes;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.ArrayList;

public class PageInfo
{

    private String url;
    private String bodyText;
    private String meta; //TODO create different fields for different metas
    private String title;
    private ArrayList<Pair<String, String>> subLinks = new ArrayList<>();

    public String getUrl()
    {
        return url;
    }

    public void setUrl(String url)
    {
        this.url = url;
    }

    public String getBodyText()
    {
        return bodyText;
    }

    public void setBodyText(String bodyText)
    {
        this.bodyText = bodyText;
    }

    public String getMeta()
    {
        return meta;
    }

    public void setMeta(Elements allMetas) //TODO Change this shit -_-
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

    public ArrayList<Pair<String,String>> getSubLinks()
    {
        return subLinks;
    }

    public void setSubLinks(ArrayList<Pair<String, String>> subLinks)
    {
        this.subLinks = subLinks;
    }
}
