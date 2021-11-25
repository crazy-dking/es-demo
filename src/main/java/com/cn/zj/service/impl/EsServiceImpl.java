package com.cn.zj.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.cn.zj.entity.Blog;
import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description: java 通过api操作es
 * @Author: wangdakai
 * @Date: 2021/11/25
 */
public class EsServiceImpl {
    public static final String INDEX = "hello";
    private  RestHighLevelClient restHighLevelClient;


    /**
     * 通过构造函数将RestHighLevelClient注入，RestHighLevelClient是用来操作es的
     */
    public EsServiceImpl(){
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost("139.9.95.160", 9200, "http"));
        // 我们通过restHighLeveClient
        restHighLevelClient = new RestHighLevelClient(restClientBuilder);
    }

    /**
     * 设置高亮。
     * 首先第一步在查询的时候这
     * @param words
     * @return
     */
    public List<Blog>  getHightLight(String words) throws IOException {
        // 请求对象。
        SearchRequest searchRequest = new SearchRequest(INDEX);
        // 请求的builder
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(words, "title", "content");
        // 将查询条件构建到构建器中。
        searchSourceBuilder.query(multiMatchQueryBuilder);
        // 高亮设置
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("title");
        highlightBuilder.preTags("<em>");
        highlightBuilder.postTags("</em>");
        searchSourceBuilder.highlighter(highlightBuilder);

        // 将查询条件设置到请求上。要不然他咋去查。
        searchRequest.source(searchSourceBuilder);
        // 发送查询请求
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        // 请求回来的记过如何获取请参照控制台发送请求到的结果。每个节点就对应一个get
        SearchHit[] hits = search.getHits().getHits();
        List<Blog> blogs = new ArrayList<>();
        for(SearchHit hit :hits){
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            HighlightField content = highlightFields.get("content");
            HighlightField title = highlightFields.get("title");
            String json = hit.getSourceAsString();
            Blog blog = JSONObject.parseObject(json, Blog.class);
            if(title!=null){
                Text[] fragments = title.getFragments();
                StringBuffer sb = new StringBuffer();
                for(Text s :fragments){
                    sb.append(s.toString());
                }
                blog.setTitle(sb.toString());
            }
            blogs.add(blog);
        }
        return blogs;
    }


    /**
     * 通过scroll方式进查询，如果scrollId是空的时候我们需要进行第一次查询，第二次的是偶就会带着scrollId，这个时候一直下一页就行了。
     * 但是这种方式不知道改如果和进行跳转到指定页。所以使用scroll的时候不提供跳页。只提供下一页。
     * @param words
     * @param scrollId
     * @param size
     * @return
     */
    public Map<String,Object> searchPageScroll(String words, String scrollId, int size) throws IOException {
        if(scrollId==null){
            SearchRequest searchRequest = new SearchRequest(INDEX);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(words, "title", "content");
            // 将查询条件构建到构建器中。
            searchSourceBuilder.query(multiMatchQueryBuilder);
            // 设置分页信息
            searchSourceBuilder.size(size);
            searchRequest.source(searchSourceBuilder);
            //设置传快照保存对的时间
            searchRequest.scroll(TimeValue.timeValueMillis(5));
            SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            Map<String,Object> map = new HashMap<>();
            map.put("scrollId",search.getScrollId());
            map.put("list",search.getHits().getHits());
            return map;
        }else{
            SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
            searchScrollRequest.scroll(TimeValue.timeValueMillis(5));
            SearchResponse scroll = restHighLevelClient.scroll(searchScrollRequest, RequestOptions.DEFAULT);
            HashMap<String, Object> map = new HashMap<>();
            map.put("scrollId",scroll.getScrollId());
            map.put("list",scroll.getHits().getHits());
            return map;
        }
    }

    /**
     * 普通分页
     * @param words
     * @param pageNum
     * @param size
     * @return
     * @throws IOException
     */
    public List<Blog> searchByPage(String words,int pageNum,int size) throws IOException {
        // 创建查询对象
        SearchRequest searchRequest = new SearchRequest(INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(words, "title", "content");
        // 将查询条件构建到构建器中。
        searchSourceBuilder.query(multiMatchQueryBuilder);
        // 设置分页信息
        searchSourceBuilder.size(size);
        searchSourceBuilder.from((pageNum-1)*size);
        // 将查询条件设置到请求上。要不然他咋去查。
        searchRequest.source(searchSourceBuilder);

        // 发送查询请求。
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = search.getHits().getHits();
        List<Blog> blogs = new ArrayList<>();
        for(SearchHit hit :hits){
            String json = hit.getSourceAsString();
            Blog blog = JSONObject.parseObject(json, Blog.class);
            blogs.add(blog);
        }
        return blogs;
    }

    public List<Blog> searchByKeywords(String words) throws IOException {
        // 创建查询对象
        SearchRequest searchRequest = new SearchRequest(INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(words, "title", "content");
        // 将查询条件构建到构建器中。
        searchSourceBuilder.query(multiMatchQueryBuilder);
        // 将查询条件设置到请求上。要不然他咋去查。
        searchRequest.source(searchSourceBuilder);
        // 发送查询请求。
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = search.getHits().getHits();
        List<Blog> blogs = new ArrayList<>();
        for(SearchHit hit :hits){
            String json = hit.getSourceAsString();
            Blog blog = JSONObject.parseObject(json, Blog.class);
            blogs.add(blog);
        }
        return blogs;
    }

    public void add(Blog blog) throws IOException {
        // IndexRequest对象用于发送es请求数据
        IndexRequest indexRequest = new IndexRequest(INDEX);
        // 设置文档id
        indexRequest.id(blog.getId()+"");
        String json = JSONObject.toJSONString(blog);
        // 将数据设置到source并设置使用json格式
        indexRequest.source(json, XContentType.JSON);
        // 发送
        restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
    }
    public Blog getBlogById(int id) throws IOException {
        GetRequest getRequest = new GetRequest(INDEX, id + "");
        GetResponse documentFields = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        // 通过id查询得到的source
        String sourceAsString = documentFields.getSourceAsString();
        Blog blog = JSONObject.parseObject(sourceAsString, Blog.class);
        return blog;
    }
    public void update(Blog blog) throws IOException {
        GetRequest getRequest = new GetRequest(INDEX, blog.getId() + "");
        // 更新的时候首先判断是否存在，
        boolean exists = restHighLevelClient.exists(getRequest, RequestOptions.DEFAULT);
        if(exists){
            UpdateRequest updateRequest = new UpdateRequest(INDEX, blog.getId() + "");
            UpdateRequest doc = updateRequest.doc(JSONObject.toJSONString(blog),XContentType.JSON);
            restHighLevelClient.update(doc,RequestOptions.DEFAULT);
        }
    }
    public  void deleteById(int id) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(INDEX, id + "");
        restHighLevelClient.delete(deleteRequest,RequestOptions.DEFAULT);
    }

    public void closeClient() throws IOException {
        restHighLevelClient.close();
    }
}
