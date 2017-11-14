package elasticsearch;

import cn.wu.domain.Article;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

/**
 * Created by lenovo on 2017/11/13.
 */
public class ESTest {

    /**
     * 直接在ELASTICSEARCH中建立文档，自动创建索引
     *
     * @throws IOException
     */
    @Test
    public void test01() throws IOException {
        // 创建链接搜索服务器对象
        Client client = getClient();
        // 描述json数据
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("id",1).
                field("title","王家卫").field("content","每个人都有过去，即使你是一个杀手，也会有小学同学").endObject();
        // 建立文档对象
        client.prepareIndex("blog1","article","1").setSource(builder).get();
        // 关闭连接
        client.close();
    }

    /**
     * 搜索在SLATICSEARCH建立的文档对象
     *
     * @throws UnknownHostException
     */
    @Test
    public void test02() throws UnknownHostException {
        // 创建连接搜索服务器对象
        Client client = getClient();
        // 搜索数据
        SearchResponse searchResponse = client.prepareSearch("blog1").
                setTypes("article").setQuery(QueryBuilders.matchAllQuery()).get();
        // 获取命中次数，查询结果有多少对象
        executeSearch(searchResponse);
        // 关闭连接
        client.close();
    }

    /**
     * 分词查询QueryStringQuery()
     *
     * @throws UnknownHostException 异常
     */
    @Test
    public void test03() throws UnknownHostException {
        // 创建连接搜索服务器对象
        Client client = getClient();
        // 搜索数据
        SearchResponse searchResponse = client.prepareSearch("blog1").
                setTypes("article").setQuery(QueryBuilders.queryStringQuery("小学")).get();
        // 获取命中次数，查询结果有多少对象
        executeSearch(searchResponse);
        // 关闭连接
        client.close();
    }

    /**
     * 模糊查询WildCardQuery()
     *
     * @throws UnknownHostException 异常
     */
    @Test
    public void test04() throws UnknownHostException {
        // 创建连接搜索服务器对象
        Client client = getClient();
        // 搜索数据
        SearchResponse searchResponse = client.prepareSearch("blog1").
                setTypes("article").setQuery(QueryBuilders.wildcardQuery("content","*小学*")).get();
        // 获取命中次数，查询结果有多少对象
        executeSearch(searchResponse);
        // 关闭连接
        client.close();
    }

    /**
     * 分词查询TermQuery()
     *
     * @throws UnknownHostException
     */
    @Test
    public void test05() throws UnknownHostException {
        // 创建连接搜索服务器对象
        Client client = getClient();
        // 搜索数据
        SearchResponse searchResponse = client.prepareSearch("blog1").
                setTypes("article").setQuery(QueryBuilders.termQuery("content","小学")).get();
        // 获取命中次数，查询结果有多少对象
        executeSearch(searchResponse);
        // 关闭连接
        client.close();
    }
    /**
     * 创建索引和添加映射
     *
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void test06() throws IOException, ExecutionException, InterruptedException {
        // 创建连接搜索服务器对象
        Client client = getClient();
        // 创建索引
        //client.admin().indices().prepareCreate("blog2").get();
        // 删除索引
        //client.admin().indices().prepareDelete("blog2").get();

        // 添加映射
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().
                startObject("article").startObject("properties").
                startObject("id").field("type", "integer").
                field("store", "yes").endObject().startObject("title").
                field("type", "string").field("store", "yes").
                endObject().startObject("content").field("type", "string").
                field("store", "yes").field("analyzer", "ik").
                endObject().endObject().endObject().endObject();
        PutMappingRequest mapping = Requests.putMappingRequest("blog2").type("article").source(builder);
        client.admin().indices().putMapping(mapping).get();
        // 关闭连接
        client.close();
    }

    /**
     * 文档相关操作
     *
     * @throws UnknownHostException
     * @throws JsonProcessingException
     */
    @Test
    public void test07() throws UnknownHostException, JsonProcessingException, ExecutionException, InterruptedException {
        // 创建连接搜索服务器对象
        Client client = getClient();
        // 封装数据
        Article article = new Article();
        article.setId(2);
        article.setTitle("从你的全世界路过");
        article.setContent("我希望有个如你一般的人,如山间清爽的风,如古城温暖的光,从清晨到夜晚,由山野到书房,只要最后是你,就好。");
        // 描述json数据
        ObjectMapper objectMapper = new ObjectMapper();
        // 创建文档
        client.prepareIndex("blog2","article",article.getId().toString()).
                setSource(objectMapper.writeValueAsString(article)).get();
        // 修改文档(使用prepareUpdate)
        /*client.prepareUpdate("blog2","article",article.getId().toString()).
                setDoc(objectMapper.writeValueAsString(article)).get();*/
        // 修改文档(使用update)
        //client.update(new UpdateRequest("blog2","article",article.getId().toString()).doc(objectMapper.writeValueAsString(article))).get();
        // 删除文档(prepareDelete)
        //client.prepareDelete("blog2","article",article.getId().toString()).get();
        // 删除文档(delete)
        //client.delete(new DeleteRequest("blog2","article",article.getId().toString())).get();
        // 关闭连接
        client.close();
    }

    /**
     * blog2批量插入100条数据
     *
     * @throws UnknownHostException
     * @throws JsonProcessingException
     */
    @Test
    public void test08() throws UnknownHostException, JsonProcessingException {
        // 创建连接搜索服务器对象
        Client client = getClient();
        ObjectMapper objectMapper = new ObjectMapper();
        for (int i = 1; i <= 100; i ++){
            Article article = new Article();
            article.setId(i);
            article.setTitle(i+"从你的全世界路过");
            article.setContent(i+"我希望有个如你一般的人,如山间清爽的风,如古城温暖的光,从清晨到夜晚,由山野到书房,只要最后是你,就好。");
            // 建立文档
            client.prepareIndex("blog2","article",article.getId().toString()).
                    setSource(objectMapper.writeValueAsString(article)).get();
        }
        // 关闭连接
        client.close();
    }

    /**
     * 分页检索
     *
     * @throws UnknownHostException
     */
    @Test
    public void test09() throws UnknownHostException {
        // 创建连接搜索服务器对象
        Client client = getClient();
        // 搜索数据
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("blog2").setTypes("article").setQuery(QueryBuilders.matchAllQuery());
        SearchResponse searchResponse = searchRequestBuilder.setFrom(20).setSize(10).get();
        executeSearch(searchResponse);
        // 关闭连接
        client.close();
    }

    private void executeSearch(SearchResponse searchResponse) {
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询结果有:"+hits.getTotalHits()+"条");
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()){
            // 查询每个对象
            SearchHit searchHit = iterator.next();
            // 获取字符串格式打印
            System.out.println(searchHit.getSourceAsString());
            System.out.println("title:"+searchHit.getSource().get("title"));
        }
    }

    private Client getClient() throws UnknownHostException {
        return TransportClient.builder().build().addTransportAddress(
                new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));
    }

}
