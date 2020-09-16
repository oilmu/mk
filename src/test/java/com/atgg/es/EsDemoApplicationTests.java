package com.atgg.es;

import jdk.management.resource.internal.inst.SocketOutputStreamRMHooks;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.naming.directory.SearchResult;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class EsDemoApplicationTests {
    /**
     * High Level Rest Client
     *      步骤：
     *          1.引入es的场景启动器
     *          2.在需要链接es的代码中创建客户端对象es
     *          3.关闭客户端
     */
    static RestHighLevelClient  restHighLevelClient;
    //其他的测试方法执行之前先初始化RestHighLevelClient对象
    @BeforeAll
    public static void init(){
        //端口号是es的端口号，java与es创建链接，创建客户端连接对象。
        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost("192.168.148.202",9200)
        );
         restHighLevelClient = new RestHighLevelClient(restClientBuilder);
    }
    //其他方法执行之后关闭客户端连接
    @AfterAll
    public static void destroy() throws IOException {
        restHighLevelClient.close();
    }

    @Test
    void contextLoads()  {

        System.out.println("restHighLevelClient = " + restHighLevelClient);

    }
    //7.search 查询
    @Test
    public void search() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //词条查询：等价于
        searchSourceBuilder.query(QueryBuilders.termQuery("price","2584"));
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
       //分页查询
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(1);
        SearchRequest searchRequest = new SearchRequest(new String[]{"sgg"}, searchSourceBuilder);
        searchRequest.types("house");
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("search = " + search);
        System.out.println("search.getHits().getTotalHits() = " + search.getHits().getTotalHits());
        for(SearchHit hit : search.getHits().getHits()){
            System.out.println("hit.getSourceAsString() = " + hit.getSourceAsString());
        }
    }
    //6.更新
    @Test
    public void update() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("sgg", "house", "10001");
        Map<String, Object> map = new HashMap<>();
        map.put("address","西山西");
        map.put("price",2584);
        updateRequest.doc(map);
        UpdateResponse update = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println("update.getResult() = " + update.getResult());
        System.out.println("update.getResult() = " + update.getShardInfo().getSuccessful());
    }

    //5.删除指定数据
    @Test
    public void delete() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("sgg", "house", "10002");
        DeleteResponse delete = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println("delete = " + delete);

    }
    //4.判断指定id的文档是否存在
    @Test
    public void exists() throws IOException {
        GetRequest getRequest = new GetRequest("sgg", "house", "10002");
        boolean exists = restHighLevelClient.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println("exists = " + exists);

    }
    //3.查询指定id的文档数据
    @Test
    public void getById() throws IOException {
        //new IndexRequest() 新增文档
        //new getRequest() 指定id查询
        //new searchRequest() 匹配搜索
        //new DeleteRequest() 指定id删除
        //new UpdateRequest() 更新数据
        GetRequest getRequest = new GetRequest("sgg", "house", "10002");
        GetResponse documentFields = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        System.out.println("documentFields = " + documentFields);
        Map<String, Object> source = documentFields.getSource();
        System.out.println(source);

    }

    //异步新增文档
    @Test
    public  void createdAsync() throws IOException, InterruptedException {
        IndexRequest indexRequest = new IndexRequest("sgg","house","10002");
        Map map = new HashMap();
        map.put("address","东山东");
        map.put("id",2);
        map.put("price",3800);
        indexRequest.source(map);
        //使用子线程异步提交请求，不会阻塞
        restHighLevelClient.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                //成功的回调方法
                System.out.println("新增成功 = " + indexResponse);
            }

            @Override
            public void onFailure(Exception e) {
                //失败的回调的方法
                System.out.println("新增失败 = " + e.getMessage());
            }
        });
        Thread.sleep(20000);
    }
    //同步新增文档，
    @Test
    void created() throws IOException {
        //新增文档使用
        IndexRequest indexRequest = new IndexRequest("sgg","house","10001");
        Map map = new HashMap();
        map.put("address","南山南");
        map.put("id",1);
        map.put("price",3500);
        indexRequest.source(map);
        IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println("indexResponse = " + indexResponse);
        //getResult:获取的是本次操作的类型
        System.out.println("indexResponse.getResult() = " + indexResponse.getResult());
        System.out.println("indexResponse.getResult() = " + indexResponse.getVersion());
    }

}
