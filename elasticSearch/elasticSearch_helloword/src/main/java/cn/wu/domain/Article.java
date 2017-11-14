package cn.wu.domain;

/**
 * 博客的实体类
 *
 * Created by lenovo on 2017/11/13.
 */
public class Article {

    private Integer id;

    private String title;

    private String Content;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return Content;
    }

    public void setContent(String content) {
        Content = content;
    }

    @Override
    public String toString() {
        return "Article{" +
                "id=" + id +
                ", title='" + title + '\'' +
                ", Content='" + Content + '\'' +
                '}';
    }
}
