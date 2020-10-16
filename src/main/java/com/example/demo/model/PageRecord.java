package com.example.demo.model;

public class PageRecord {

    Long viewtime;
    String userid;
    String pageid;

    public PageRecord() {
    }

    public Long getViewtime() {
        return viewtime;
    }

    public PageRecord(Long viewtime, String userid, String pageid) {
        this.viewtime = viewtime;
        this.userid = userid;
        this.pageid = pageid;
    }

    public String getUserid() {
        return userid;
    }

    public String getPageid() {
        return pageid;
    }

    public String toString() {
        return new com.google.gson.Gson().toJson(this);
    }
}
