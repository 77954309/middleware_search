package com.search.innerservice;

import com.alibaba.fastjson.JSON;
import com.search.model.FieldInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

/**
 * TransportClient 属性操作
 *
 * @author limeng
 * @create 2018-08-13 下午5:30
 **/
public class ESInnerService {
    private static final Logger logger = Logger.getLogger(ESInnerService.class);
    private TransportClient client;


    /**
     * 为集群添加新的节点
     * @param name
     */
    public  void addNode(String name) {
        try {
            this.client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(name), 9300));
        } catch (UnknownHostException e) {
            logger.error("集群添加新的节点失败:"+e.getMessage());
        }
    }

    /**
     * 删除集群中的某个节点
     * @param name
     */
    public  void removeNode(String name) {
        try {
            this.client.removeTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(name), 9300));
        } catch (UnknownHostException e) {
            logger.error("删除集群中的某个节点失败:"+e.getMessage());
        }
    }


    /**
     * 创建mapping
     * @param index    索引
     * @param type     类型
     * @param xMapping mapping描述
     */
    public void createBangMapping(String index, String type, XContentBuilder xMapping) {
        PutMappingRequest mapping = Requests.putMappingRequest(index).type(type).source(xMapping);
        this.client.admin().indices().putMapping(mapping).actionGet();

    }

    /**
     * 创建索引
     *
     * @param index   索引名称
     */
    public void createIndex(String index) {
        CreateIndexRequest request = new CreateIndexRequest(index);
        this.client.admin().indices().create(request);
    }


    /**
     * 根据信息自动创建索引与mapping
     * 构建mapping描述    有问题
     * @param fieldInfoList  字段信息
     * @return
     */
    public void createIndexAndCreateMapping(String index, String type, List<FieldInfo> fieldInfoList) {
        XContentBuilder mapping = null;
        try {
            CreateIndexRequestBuilder cib=this.client.admin()
                    .indices().prepareCreate(index);
            //设置之定义字段
            mapping = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties");
            for(FieldInfo info : fieldInfoList){
                String field = info.getField();
                String dateType = info.getType();
                if(dateType == null || "".equals(dateType.trim())){
                    dateType = "String";
                }
                dateType = dateType.toLowerCase();
                Integer participle = info.getParticiple();
                /**
                 * analyzer 分词策略
                 * ik_max_word：会将文本做最细粒度的拆分，例如「中华人民共和国国歌」会被拆分为「中华人民共和国、中华人民、中华、华人、人民共和国、人民、人、民、共和国、共和、和、国国、国歌」，会穷尽各种可能的组合
                 * ik_smart：会将文本做最粗粒度的拆分，例如「中华人民共和国国歌」会被拆分为「中华人民共和国、国歌」
                 * **/
                if("string".equals(dateType)){
                    if(participle == 1) {
                        mapping.startObject(field)
                                .field("type","text")
                                .field("analyzer","ik_smart")
                                .endObject();
                    }else if(participle == 2){
                        mapping.startObject(field)
                                .field("type","text")
                                .field("analyzer","ik_max_word")
                                .endObject();
                    }else {
                        mapping.startObject(field)
                                .field("type","keyword")
                                .field("index","not_analyzed")
                                .endObject();
                    }

                }else if("date".equals(dateType)){
                    mapping.startObject(field)
                            .field("type",dateType)
                            .field("format","yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis")
                            .endObject();
                }else {
                    mapping.startObject(field)
                            .field("type",dateType)
                            .field("index","not_analyzed")
                            .endObject();
                }

            }
            mapping.endObject()
                    .endObject();
            cib.addMapping(type, mapping);
            cib.execute().actionGet();
        } catch (IOException e) {
            logger.error("创建索引发生失败:"+e.getMessage());
        }
    }
    /**
     * 根据信息自动创建索引与mapping
     * 构建mapping描述    有问题
     * String json="[{\"field\":\"id\",\"type\":\"integer\"},{\"field\":\"desc\",\"participle\":2,\"type\":\"string\"}]";
     * @param fieldInfoJson  字段信息
     */
    public void createIndexAndCreateMapping(String index, String type, String fieldInfoJson){
        if(StringUtils.isNotBlank(fieldInfoJson)){
            List<FieldInfo> fieldInfoList = JSON.parseArray(fieldInfoJson, FieldInfo.class);
            if(fieldInfoList!=null && !fieldInfoList.isEmpty()){
                this.createIndexAndCreateMapping(index,type,fieldInfoList);
            }
        }
    }


    /**
     * 删除索引
     * @param indexName 索引
     */
    public  void deleteIndex(String indexName) {
        try {
            if (!isIndexExists(indexName)) {
                logger.warn("索引不存在:"+indexName);
            } else {

                DeleteIndexResponse dResponse = this.client.admin().indices().prepareDelete(indexName)
                        .execute().actionGet();
                if (dResponse.isAcknowledged()) {
                    logger.info("删除索引成功:"+indexName);
                }else{
                    logger.error("删除索引失败:"+indexName);
                }
            }
        } catch (Exception e) {
            logger.error("删除索引失败:"+e.getMessage());
        }
    }

    /**
     * 判断索引是否存在 传入参数为索引库名称
     * @param indexName 索引
     * @return
     */
    public  boolean isIndexExists(String indexName) {
        boolean flag = false;
        try {
            IndicesExistsRequest inExistsRequest = new IndicesExistsRequest(indexName);
            IndicesExistsResponse inExistsResponse = this.client.admin().indices()
                    .exists(inExistsRequest).actionGet();

            if (inExistsResponse.isExists()) {
                flag = true;
            } else {
                flag = false;
            }
        } catch (Exception e) {
            logger.error("判断索引失败:"+e.getMessage());
        }

        return flag;
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
