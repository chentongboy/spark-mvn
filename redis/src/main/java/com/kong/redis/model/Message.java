package com.kong.redis.model;

import java.io.Serializable;

/**
 * 消息bean
 * Created by kong on 2016/4/30.
 */
public class Message implements Serializable{
    private String id;
    private String other;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getOther() {
        return other;
    }

    public void setOther(String other) {
        this.other = other;
    }
}
