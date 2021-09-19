package com.tqk.rocketmqdemo.myenum;

public enum  TqkEnum {
    IPPORT("110.42.146.236:9876");
    String msg;
    private TqkEnum(String msg)
    {
        this.msg = msg;
    }

    public String getMsg() {
        return msg;
    }
}