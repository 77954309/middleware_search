package com.search.utils;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 资源文件工具
 *
 * @author limeng
 * @create 2018-08-13 下午4:24
 **/
public class PropertiesUtils {
    private static final Logger logger = Logger.getLogger(PropertiesUtils.class);
    public static Properties getProperties(){
        InputStream resourceAsStream = PropertiesUtils.class.getClass().getResourceAsStream("/connection.properties");
        Properties properties=new Properties();
        try {
            properties.load(resourceAsStream);
        } catch (IOException e) {
            logger.error("获取资源文件失败:"+e.getMessage());
        }
        return properties;
    }
}
