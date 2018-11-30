package com.search.model;

import java.util.List;

/**
 * 字段属性操作
 *
 * @author limeng
 * @create 2018-08-14 下午4:02
 **/
public class FieldOperating {
    //索引
    private String index;
    //字段类型
    private String type;
    //数据
    private FieldDataJson fieldDataJson;
    //数据
    private List<FieldDataJson> fieldDataJsonList;
    //数据路径
    private String dataPath;

    public FieldOperating() {
    }

    /**
     * 数据
     */
    public static class FieldDataJson{
        //_id
        private String id;
        private String dataJson;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getDataJson() {
            return dataJson;
        }

        public void setDataJson(String dataJson) {
            this.dataJson = dataJson;
        }
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public FieldDataJson getFieldDataJson() {
        return fieldDataJson;
    }

    public void setFieldDataJson(FieldDataJson fieldDataJson) {
        this.fieldDataJson = fieldDataJson;
    }

    public List<FieldDataJson> getFieldDataJsonList() {
        return fieldDataJsonList;
    }

    public void setFieldDataJsonList(List<FieldDataJson> fieldDataJsonList) {
        this.fieldDataJsonList = fieldDataJsonList;
    }

    public String getDataPath() {
        return dataPath;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }
}
