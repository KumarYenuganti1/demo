package com.example.demo.model;

public class DataRecord2 {

    Long count;

    public DataRecord2() {
    }

    public DataRecord2(Long count) {
        this.count = count;
    }

    public Long getCount() {
        return count;
    }

    public String toString() {
        return new com.google.gson.Gson().toJson(this);
    }

}
