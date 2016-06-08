package com.kong.design.create.factory.general;

/**
 * Created by kong on 2016/4/26.
 */
public class SMSSender implements Sender {
    @Override
    public void send() {
        System.out.println("smsSender send");
    }
}
