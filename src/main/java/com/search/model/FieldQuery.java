package com.search.model;

import org.elasticsearch.index.query.QueryBuilder;

import java.util.List;

/**
 * 查询
 *
 * @author limeng
 * @create 2018-08-14 下午7:14
 **/
public class FieldQuery {
    //索引集合
    private List<String> indexs;
    //类型
    private List<String> types;
    //id
    private List<String> ids;
    //查询
    private QueryBuilder queryBuilders;
    //分页
    private Integer from;
    //查询最大条数
    private Integer size;


    public List<String> getIndexs() {
        return indexs;
    }

    public void setIndexs(List<String> indexs) {
        this.indexs = indexs;
    }

    public List<String> getTypes() {
        return types;
    }

    public void setTypes(List<String> types) {
        this.types = types;
    }

    public List<String> getIds() {
        return ids;
    }

    public void setIds(List<String> ids) {
        this.ids = ids;
    }

    public QueryBuilder getQueryBuilders() {
        return queryBuilders;
    }

    public void setQueryBuilders(QueryBuilder queryBuilders) {
        this.queryBuilders = queryBuilders;
    }


    public Integer getFrom() {
        return from;
    }

    public void setFrom(Integer from) {
        this.from = from;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }


}
