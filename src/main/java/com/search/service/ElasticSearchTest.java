package com.search.service;



import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.search.innerservice.ESCrudInnerService;
import com.search.model.FieldQuery;
import com.search.utils.ESConnection;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.util.*;


/**
 * 测试类
 *
 * @author limeng
 * @create 2018-08-13 下午3:26
 **/
public class ElasticSearchTest {


    /**
     * 创建索引与mapping模板
     * @param index 索引字段
     * @param type  类型
     * @param client  客户端
     * @throws IOException
     */
    public void createMapping(String index, String type,TransportClient client) throws IOException {

        CreateIndexRequestBuilder cib=client.admin()
                .indices().prepareCreate(index);
        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties") //设置之定义字段


                .startObject("id")//字段id
                .field("type","integer")//设置数据类型
                .field("index","not_analyzed")

                .endObject()
                .startObject("classs")
                .field("type","integer")
                .field("index","not_analyzed")
                .endObject()

                .startObject("courseClass")
                .field("type","integer")
                .field("index","not_analyzed")
                .endObject()

                .startObject("courseClassExam")
                .field("type","integer")
                .field("index","not_analyzed")
                .endObject()

                .startObject("examnum")
                .field("type","integer")
                .field("index","not_analyzed")
                .endObject()

                .startObject("ok")
                .field("type","integer")
                .field("index","not_analyzed")
                .endObject()

                .startObject("room")
                .field("type","integer")
                .field("index","not_analyzed")
                .endObject()

                .startObject("score")
                .field("type","integer")
                .field("index","not_analyzed")
                .endObject()

                .startObject("student")
                .field("type","integer")
                .field("index","not_analyzed")
                .endObject()

                .startObject("updatetime")
                .field("type","integer")
                .field("index","not_analyzed")
                .endObject()

                .startObject("desc")
                .field("type","text")
                .field("analyzer","ik_smart")//ik_max_word
                .endObject()

                .startObject("name")
                .field("type","string")
                .field("index","not_analyzed")
                .endObject()
                .endObject()
                .endObject();
        cib.addMapping(type, mapping);
        cib.execute().actionGet();
    }



    public static void main(String[] args) {
        //读取配置文件连接信息
        //ESConnection esConnection = new ESConnection();
        //自定义连接信息
        ESConnection esConnection = new ESConnection("my-application","xxx",9300);
        //ESInnerService eSInnerService = esConnection.getESInnerService(esConnection);
        ESCrudInnerService eSCrudInnerService = esConnection.getESCrudInnerService(esConnection);

        //json模式动态创建索引
        //String json="[{\"field\":\"id\",\"type\":\"integer\"},{\"field\":\"desc\",\"participle\":2,\"type\":\"string\"}]";
        //List<FieldInfo> fieldInfos = JSON.parseArray(json, FieldInfo.class);
        //System.out.println(fieldInfos);

        //eSInnerService.createIndexAndCreateMapping("test20180813","test",fieldInfoList);

        //删除索引
        //eSInnerService.deleteIndex("test20180813");
        //动态创建索引
        /*List<FieldInfo> fieldInfoList=new ArrayList<FieldInfo>();
        FieldInfo fieldInfo1=new FieldInfo();
        fieldInfo1.setField("id");
        fieldInfo1.setType("integer");

        FieldInfo fieldInfo2=new FieldInfo();
        fieldInfo2.setField("desc");
        fieldInfo2.setType("string");
        fieldInfo2.setParticiple(2);
        fieldInfoList.add(fieldInfo1);
        fieldInfoList.add(fieldInfo2);
        eSInnerService.createIndexAndCreateMapping("test20180813","test",fieldInfoList);*/


        //插入数据
       /* FieldOperating fieldOperating = new FieldOperating();
        FieldOperating.FieldDataJson fieldDataJson = new FieldOperating.FieldDataJson();
        fieldOperating.setIndex("test20180813");
        fieldOperating.setType("test");
        fieldDataJson.setId("33");
        fieldDataJson.setDataJson("{\"id\":\"33\",\"desc\":\"abcd\"}");
        fieldOperating.setFieldDataJson(fieldDataJson);
        eSCrudInnerService.add(fieldOperating);*/


        //缓存批量插入数据
        /*FieldOperating fieldOperating = new FieldOperating();

        fieldOperating.setIndex("test20180813");
        fieldOperating.setType("test");

        List<FieldOperating.FieldDataJson> dataJsonList=new ArrayList<FieldOperating.FieldDataJson>();
        for (int i = 0; i < 10; i++) {
            String id="4"+String.valueOf(i);
            String dataJson="{\"id\":\""+id+"\",\"desc\":\"KUU依托于儿童\"}";
            FieldOperating.FieldDataJson fieldDataJson = new FieldOperating.FieldDataJson();
            fieldDataJson.setId(id);
            fieldDataJson.setDataJson(dataJson);
            dataJsonList.add(fieldDataJson);
        }

        fieldOperating.setFieldDataJsonList(dataJsonList);
        BulkProcessor bulkProcessor = eSCrudInnerService.autoBulkProcessor();
        eSCrudInnerService.cacheAdd(fieldOperating,bulkProcessor);*/

        //批量插入
       /* FieldOperating fieldOperating = new FieldOperating();
        fieldOperating.setIndex("test20180813");
        fieldOperating.setType("test");
        List<FieldOperating.FieldDataJson> list=new ArrayList<FieldOperating.FieldDataJson>();
        for (int i = 0; i < 10; i++) {
            String id="2"+String.valueOf(i);
            String dataJson="{\"id\":\""+id+"\",\"desc\":\"奥术大师恐龙当家阿斯利康大街上的卢卡斯建档立卡来看大家来速度快辣椒水\"}";
            FieldOperating.FieldDataJson fieldDataJson = new FieldOperating.FieldDataJson();
            fieldDataJson.setId(id);
            fieldDataJson.setDataJson(dataJson);
            list.add(fieldDataJson);
        }
        fieldOperating.setFieldDataJsonList(list);
        eSCrudInnerService.batchByJson(fieldOperating);*/


       //删除
        /*FieldOperating fieldOperating = new FieldOperating();
        FieldOperating.FieldDataJson fieldDataJson = new FieldOperating.FieldDataJson();
        fieldOperating.setIndex("test20180813");
        fieldOperating.setType("test");
        fieldDataJson.setId("22");
        fieldOperating.setFieldDataJson(fieldDataJson);
        eSCrudInnerService.deleteById(fieldOperating);*/


        //批量查询
        /*FieldOperating fieldOperating = new FieldOperating();
        FieldOperating.FieldDataJson fieldDataJson = new FieldOperating.FieldDataJson();
        fieldOperating.setIndex("test20180813");
        fieldOperating.setType("test");
        List<String> strings = eSCrudInnerService.searchByType(fieldOperating);
        System.out.println(Arrays.toString(strings.toArray()));*/

        //根据_id查询
        /*FieldOperating fieldOperating = new FieldOperating();
        FieldOperating.FieldDataJson fieldDataJson = new FieldOperating.FieldDataJson();
        fieldOperating.setIndex("test20180813");
        fieldOperating.setType("test");
        fieldDataJson.setId("23");
        fieldOperating.setFieldDataJson(fieldDataJson);
        String strings = eSCrudInnerService.searchByIndexAndId(fieldOperating);
        System.out.println(strings);*/


        //查询
        /**
         * elasticsearch 里默认的IK分词器是会将每一个中文都进行了分词的切割，所以你直接想查一整个词，或者一整句话是无返回结果的。 match,term都有这种情况
         * 可以设置多个词语拼装查询或者字段设置不分词
         *
         **/

        //查询最大范围
       /* QueryBuilder queryBuilder =QueryBuilders.spanFirstQuery(
                QueryBuilders.spanTermQuery("desc", "奥"),  // Query
                30000                                             // Max查询范围的结束位置
        );*/
       //匹配当前词语
        /*QueryBuilder queryBuilder = QueryBuilders.termQuery("desc", "奥");
        QueryBuilder queryBuilder2 = QueryBuilders.termQuery("desc", "大");
        BoolQueryBuilder bool = QueryBuilders.boolQuery();
        bool.should(queryBuilder);
        bool.should(queryBuilder2);*/


        //模糊查询
        //QueryBuilder queryBuilder = QueryBuilders.fuzzyQuery("desc", "abcd");

        //正则匹配 使用正则不要分词
        //QueryBuilder queryBuilder = QueryBuilders.regexpQuery("id","");

        //通配符查询 不要分词
       // QueryBuilder queryBuilder = QueryBuilders.wildcardQuery("author", "*e");


        List<String> indexs=new ArrayList<String>();
        indexs.add("logstash-custom-2018.11.25");

        List<String> types=new ArrayList<String>();
        types.add("logs");

        FieldQuery fieldQuery = new FieldQuery();
        fieldQuery.setIndexs(indexs);
        fieldQuery.setTypes(types);
        //fieldQuery.setType("test");
        //fieldQuery.setQueryBuilders(queryBuilder);
        fieldQuery.setFrom(0);
        fieldQuery.setSize(10);

        List<String> strings =eSCrudInnerService.searchByQueryBuilder(fieldQuery);
        System.out.println(Arrays.toString(strings.toArray()));


    }
}
