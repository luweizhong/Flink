package com.stream.flink.entity;

public class ItemEntity{
    private int id;

    private String name;

    private long ts;


    public ItemEntity() {
    }

    public ItemEntity(int id, String name, long ts) {
        this.id = id;
        this.name = name;
        this.ts = ts;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }
}
