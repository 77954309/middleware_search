package com.search.utils;

import com.search.innerservice.ESCrudInnerService;
import com.search.innerservice.ESInnerService;
import org.apache.log4j.Logger;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

/**
 * es工具类
 *
 * @author limeng
 * @create 2018-08-13 下午4:19
 **/
public class ESConnection {
    private static final Logger logger = Logger.getLogger(ESConnection.class);
    public static final  String fieldType = "type";
    private String clusterName;
    private String ip;
    private int port;
    private TransportClient client;


    public ESConnection() {
        if(this.getClient()==null){
            Properties properties = PropertiesUtils.getProperties();
            this.clusterName=properties.getProperty("es.clustername");
            this.ip=properties.getProperty("es.ips");
            this.port=Integer.valueOf(properties.getProperty("es.port"));
        }
    }

    public ESConnection(String clusterName,String ip, int port) {
        this.clusterName = clusterName;
        this.ip = ip;
        this.port = port;
    }

    /**
     * 获取工具类的实例
     * @return
     */
    public ESInnerService getESInnerService(ESConnection eSConnection){
        if(eSConnection==null){
            return null;
        }
        eSConnection.getTransportClient();
        ESInnerService eSInnerService = new ESInnerService();
        eSInnerService.setClient(eSConnection.client);
        return eSInnerService;
    }

    /**
     * 获取工具类的实例
     * @return
     */
    public ESCrudInnerService getESCrudInnerService(ESConnection eSConnection){
        if(eSConnection==null){
            return null;
        }
        eSConnection.getTransportClient();
        ESCrudInnerService eSCrudInnerService = new ESCrudInnerService();
        eSCrudInnerService.setClient(eSConnection.client);
        return eSCrudInnerService;
    }

    /**
     * 取得实例
     * @return
     */
    public void getTransportClient() {
        if(this.client!=null){
            return;
        }
        TransportClient client = null ;
        try {
            //client.transport.ignore_cluster_name忽略集群名称校验
            //  client.transport.sniff根据集群名称自动发现节点
            Settings settings = Settings.builder().put("cluster.name", this.clusterName).put("client.transport.ping_timeout", "30s").build();
            client = new PreBuiltTransportClient(settings);
            String[] ips = this.ip.split(",");
            for (int i = 0; i < ips.length; i++) {
                client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ips[i]), this.port));
            }
        } catch (UnknownHostException e) {
            logger.error("创建实例失败:"+e.getMessage());
        }
        this.client=client;
    }

    /**
     * 关闭连接
     * @param client
     */
    public void close(TransportClient client) {
        client.close();
    }


    public void setField(String clusterName, String ip, int port) {
        this.clusterName = clusterName;
        this.ip = ip;
        this.port = port;
    }


    public TransportClient getClient() {
        return client;
    }

    public void setClient(TransportClient client) {
        if(this.client==null){
            this.client = client;
        }
    }
}
