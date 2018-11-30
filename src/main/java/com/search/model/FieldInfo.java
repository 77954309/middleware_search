package com.search.model;

/**
 * 字段信息
 *
 * @author limeng
 * @create 2018-08-13 下午3:30
 **/
public class FieldInfo {
    //字段名称
    private String field;
    //字段类型
    private String type;
    //分词器策略
    private Integer participle;

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Integer getParticiple() {
        return participle;
    }

    public void setParticiple(Integer participle) {
        this.participle = participle;
    }
}
