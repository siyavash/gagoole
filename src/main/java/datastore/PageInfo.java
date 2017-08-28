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
    private String titleMeta;
    private String descriptionMeta;
    private String keyWordsMeta;
    private String authorMeta;
    private String contentTypeMeta;
    private String title;
    private ArrayList<Pair<String, String>> subLinks;

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

    public String getTitleMeta()
    {
        return titleMeta;
    }

    public void setTitleMeta(String titleMeta)
    {
        this.titleMeta = titleMeta;
    }

    public String getDescriptionMeta()
    {
        return descriptionMeta;
    }

    public void setDescriptionMeta(String descriptionMeta)
    {
        this.descriptionMeta = descriptionMeta;
    }

    public String getKeyWordsMeta()
    {
        return keyWordsMeta;
    }

    public void setKeyWordsMeta(String keyWordsMeta)
    {
        this.keyWordsMeta = keyWordsMeta;
    }

    public String getAuthorMeta()
    {
        return authorMeta;
    }

    public void setAuthorMeta(String authorMeta)
    {
        this.authorMeta = authorMeta;
    }

    public String getContentTypeMeta()
    {
        return contentTypeMeta;
    }

    public void setContentTypeMeta(String contentTypeMeta)
    {
        this.contentTypeMeta = contentTypeMeta;
    }
}
