package com.search.innerservice;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.search.model.FieldOperating;
import com.search.model.FieldQuery;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *
 * TransportClient 增删改查
 * @author limeng
 * @create 2018-08-14 下午4:07
 **/
public class ESCrudInnerService {
    private static final Logger logger = Logger.getLogger(ESInnerService.class);
    private TransportClient client;


    /**
     * 设置自动提交文档的实例
     * @return BulkProcessor
     */
    public BulkProcessor autoBulkProcessor(){
        BulkProcessor bulkProcessor = BulkProcessor.builder(this.client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId,
                                           BulkRequest request) {
                        //提交前调用

                    }
                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          BulkResponse response) {
                        //提交结束后调用（无论成功或失败）
                        logger.warn("提交" + response.getItems().length + "个文档，用时"+ response.getTookInMillis() + "MS" + (response.hasFailures() ? " 有文档提交失败！" : ""));

                    }
                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          Throwable failure) {

                        logger.error("有文档提交失败:"+failure.getMessage());

                    }
                })
                //当请求超过10000个（default=1000）或者总大小超过1GB（default=5MB）时，触发批量提交动作。
                .setBulkActions(10000)//文档数量达到1000时提交
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))//总文档体积达到5MB时提交
                .setFlushInterval(TimeValue.timeValueSeconds(5))//每5S提交一次（无论文档数量、体积是否达到阈值）
                .setConcurrentRequests(1)//加1后为可并行的提交请求数，即设为0代表只可1个请求并行，设为1为2个并行
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();

        return bulkProcessor;
    }

    /**
     * 增加单条
     * @param fieldOperating 字段
     */
    public void add(FieldOperating fieldOperating){
        if(fieldOperating!=null){
            String index=fieldOperating.getIndex();
            String type=fieldOperating.getType();

            BulkRequestBuilder bulkRequest = this.client.prepareBulk();
            FieldOperating.FieldDataJson dataJson=fieldOperating.getFieldDataJson();
            if(dataJson == null){
                logger.error("add数据对象json为空:"+new Date());
            }else{
                //添加文档，以便自动提交
                bulkRequest.add(this.client.prepareIndex(index, type).setSource(dataJson.getDataJson(),XContentType.JSON).setId(dataJson.getId()));
                // 一次执行插入数据的操作
                BulkResponse response = bulkRequest.execute().actionGet();
                if(response.hasFailures()){
                    logger.error("插入失败:"+response.buildFailureMessage()+new Date());
                }
            }

        }else{
            logger.error("add数据对象为空:"+new Date());
        }
    }

    /**
     * 设置提交文档最大数量，在方法autoBulkProcessor 到达一定条件出发自动提交
     * @param fieldOperating
     * @param bulkProcessor
     */
    public void cacheAdd(FieldOperating fieldOperating,BulkProcessor bulkProcessor){
        if(fieldOperating!=null && bulkProcessor!=null){
            String id="";
            String index=fieldOperating.getIndex();
            String type=fieldOperating.getType();
            List<FieldOperating.FieldDataJson> fieldDataJsonList = fieldOperating.getFieldDataJsonList();
            if(fieldDataJsonList.isEmpty()){
                logger.error("cacheAdd数据对象json为空:"+new Date());
            }else{
                for(FieldOperating.FieldDataJson fieldDataJson:fieldDataJsonList){
                    id="";
                    if(fieldDataJson!=null){
                        id=fieldDataJson.getId();
                        if(StringUtils.isNotBlank(id)){
                            bulkProcessor.add(new IndexRequest(index, type,id).source(fieldDataJson.getDataJson(),XContentType.JSON));//添加文档，以便自动提交
                        }else{
                            bulkProcessor.add(new IndexRequest(index, type).source(fieldDataJson.getDataJson(),XContentType.JSON));//添加文档，以便自动提交
                        }

                    }
                }
            }

        }else{
            logger.error("cacheAdd数据对象为空:"+new Date());
        }

    }


    /**
     * 批量插入 数据在json
     * @param fieldOperating 数据
     */
    public void batchByJson(FieldOperating fieldOperating){
        BulkRequestBuilder bulkRequest = this.client.prepareBulk();
        List<FieldOperating.FieldDataJson> fieldDataJsonList = fieldOperating.getFieldDataJsonList();
        if(fieldDataJsonList.isEmpty()){
            logger.error("batch数据对象为空:"+new Date());
        }else{
            String index=fieldOperating.getIndex();
            String type=fieldOperating.getType();
            for (FieldOperating.FieldDataJson dataJson:fieldDataJsonList) {
                if(dataJson!=null){
                    IndexRequestBuilder indexRequest = this.client.prepareIndex(index, type)
                            //指定不重复的ID
                            .setSource(dataJson.getDataJson(), XContentType.JSON).setId(dataJson.getId());
                    //添加到builder中
                    bulkRequest.add(indexRequest);
                }
            }

            // 一次执行插入数据的操作
            BulkResponse response = bulkRequest.execute().actionGet();
            if(response.hasFailures()){
                logger.error("batchByJson插入失败:"+response.buildFailureMessage()+new Date());
            }
        }

    }

    /**
     * 批量插入 数据在file
     * @param fieldOperating 数据
     */
    public void batchByFile(FieldOperating fieldOperating){
        BulkRequestBuilder bulkRequest = this.client.prepareBulk();
        FileReader fr = null;
        BufferedReader bfr = null;
        String line=null;

        if(fieldOperating!=null){
            String index=fieldOperating.getIndex();
            String type=fieldOperating.getType();
            String fieldPath=fieldOperating.getDataPath();

            File file = new File(fieldPath);
            try {
                fr=new FileReader(file);
                bfr=new BufferedReader(fr);
                int count=0;
                while((line=bfr.readLine())!=null){
                    bulkRequest.add(this.client.prepareIndex(index,type).setSource(line,XContentType.JSON));
                    if (count%10==0) {
                        bulkRequest.execute().actionGet();
                    }
                    count++;
                }
                bulkRequest.execute().actionGet();
            }catch (Exception e){
                logger.error("batchByFile插入失败:"+e.getMessage()+new Date());
            }finally {
                try {
                    if (bfr != null) {
                        bfr.close();
                    }
                    if (fr != null) {
                        fr.close();
                    }
                } catch (IOException e) {
                    logger.error("batchByFile插入失败:"+e.getMessage()+new Date());
                }
            }

        }else{
            logger.error("batchByFile数据对象为空:"+new Date());
        }

    }

    /**
     * 根据id删除索引
     * @param fieldOperating 数据
     */
    public void deleteById(FieldOperating fieldOperating){
        if(fieldOperating!=null){
            String index=fieldOperating.getIndex();
            String type=fieldOperating.getType();
            FieldOperating.FieldDataJson fieldDataJson = fieldOperating.getFieldDataJson();
            DeleteResponse dResponse = this.client.prepareDelete(index,type, fieldDataJson.getId()).execute().actionGet();
            if ("OK".equals(dResponse.status())) {
                logger.info("deleteById删除成功:"+new Date());
            } else {
                logger.error("deleteById删除失败:"+new Date());
            }
        }else{
            logger.error("deleteById数据对象为空:"+new Date());
        }

    }





    /**
     * 根据index、type、id进行查询
     * @param fieldOperating 数据
     */
    public String searchByIndexAndId(FieldOperating fieldOperating){
        String json="";
        if(fieldOperating!=null) {
            String index=fieldOperating.getIndex();
            String type = fieldOperating.getType();
            FieldOperating.FieldDataJson fieldDataJson = fieldOperating.getFieldDataJson();
            GetResponse response = this.client.prepareGet(index,type,fieldDataJson.getId()).execute()
                    .actionGet();
            json = response.getSourceAsString();

        }else{
            logger.error("searchByIndex数据对象为空:"+new Date());
        }
        return json;
    }

    /**
     * 批量查询 根据index、type查询
     * @param fieldOperating 数据
     * @return
     */
    public  List<String> searchByType(FieldOperating fieldOperating){
        List<String> jsonList=new ArrayList<String>();
        if(fieldOperating!=null) {
            String index=fieldOperating.getIndex();
            String type = fieldOperating.getType();
            SearchResponse response = this.client.prepareSearch(index).setTypes(type).get();

            for (SearchHit searchHit: response.getHits()) {
                if(searchHit.hasSource()){
                    jsonList.add(searchHit.getSourceAsString());
                }
            }
        }else{
            logger.error("searchByType数据对象为空:"+new Date());
        }
        return jsonList;
    }

    /**
     * elasticsearch 里默认的IK分词器是会将每一个中文都进行了分词的切割，所以你直接想查一整个词，或者一整句话是无返回结果的。 match,term都有这种情况
     * 可以设置多个词语拼装查询或者字段设置不分词
     *
     * 使用QueryBuilder
     * termQuery("key", obj) 完全匹配
     * termsQuery("key", obj1, obj2..)   一次匹配多个值
     * matchQuery("key", Obj) 单个匹配, field不支持通配符, 前缀具高级特性
     * multiMatchQuery("text", "field1", "field2"..);  匹配多个字段, field有通配符忒行
     * matchAllQuery();         匹配所有文件
     *
     * 组合查询
     * must(QueryBuilders) :   AND
     * mustNot(QueryBuilders): NOT
     * should:                  : OR
     *
     * 只查询一个id的
     *  QueryBuilder queryBuilder = QueryBuilders.idsQuery().ids("1");
     *
     * 包裹查询, 高于设定分数, 不计算相关性
     * QueryBuilder queryBuilder = QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("name", "kimchy")).boost(2.0f);
     *
     * 模糊查询 不能用通配符, 不知道干啥用
     *  QueryBuilder queryBuilder = QueryBuilders.fuzzyQuery("user", "kimch");
     *
     *  前缀查询
     *QueryBuilder queryBuilder = QueryBuilders.matchQuery("user", "kimchy");
     *
     *  范围内查询
     *   QueryBuilder queryBuilder = QueryBuilders.rangeQuery("price")
     *.from(1)
     *.to(100)
     *.includeLower(false)
     *.includeUpper(false);
     *
     * 通配符查询, 支持 *
     *  匹配任何字符序列, 包括空
     *  避免* 开始, 会检索大量内容造成效率缓慢
     *  QueryBuilder queryBuilder = QueryBuilders.wildcardQuery("user", "ki*hy");
     *
     *  QueryBuilder qb = QueryBuilders.regexpQuery(
     *  "title",
     *   "*J");
     *
     *  多id查询
     *   QueryBuilder queryBuilder = QueryBuilders.idsQuery("")
     *  .addIds("512", "520", "531");
     *
     *  类型查询
     *  QueryBuilder queryBuilder = QueryBuilders.typeQuery("data");
     */
    public List<String> searchByQueryBuilder(FieldQuery fieldQuery){
        List<String> jsonList=new ArrayList<String>();
        if(fieldQuery!=null){
            //增加索引
            List<String> indexs = fieldQuery.getIndexs();
            StringBuilder indices=new StringBuilder();
            if(!indexs.isEmpty()){
                for (String index:indexs){
                    if(StringUtils.isNotBlank(index)){
                        indices.append(index).append(",");
                    }
                }
            }else{
                logger.error("searchByQueryBuilder索引为空:"+new Date());
                return null;
            }
            SearchRequestBuilder searchRequestBuilder = this.client.prepareSearch(indices.deleteCharAt(indices.length() - 1).toString());

            //增加类型
            List<String> typeList = fieldQuery.getTypes();
            if(typeList!=null && !typeList.isEmpty()){
                StringBuilder types=new StringBuilder();
                for (String type:typeList){
                    if(StringUtils.isNotBlank(type)){
                        types.append(type).append(",");
                    }
                }
                searchRequestBuilder.setTypes(types.deleteCharAt(types.length() - 1).toString());
            }

            //查询对象
            if(fieldQuery.getQueryBuilders() != null){
                searchRequestBuilder.setQuery(fieldQuery.getQueryBuilders());
            }

            //分页
            if(fieldQuery.getFrom()!=null){
                searchRequestBuilder.setFrom(fieldQuery.getFrom());
            }
            if(fieldQuery.getSize()!=null){
                searchRequestBuilder.setSize(fieldQuery.getSize());
            }

            SearchResponse response = searchRequestBuilder.get();
            for (SearchHit searchHit : response.getHits()) {
                if(searchHit.hasSource()){
                    jsonList.add(searchHit.getSourceAsString());
                }
            }

        }else{
            logger.error("searchByQueryBuilder数据对象为空:"+new Date());
        }
        return jsonList;
    }



    /**
     * 内部有连接池，单例的
     * @param client TransportClient
     */
    public void setClient(TransportClient client) {
        if(this.client==null){
            this.client = client;
        }
    }
}
