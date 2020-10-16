package com.example.demo.model;

public class StudentRecord {

    Long studentId;

    public StudentRecord() {
    }

    public StudentRecord(Long count) {
        this.studentId = count;
    }

    public Long getCount() {
        return studentId;
    }

    public String toString() {
        return new com.google.gson.Gson().toJson(this);
    }
}
