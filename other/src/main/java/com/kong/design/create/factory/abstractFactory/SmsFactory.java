package com.kong.design.create.factory.abstractFactory;

/**
 * Created by kong on 2016/4/26.
 */
public class SmsFactory implements Provider {
    @Override
    public Sender produce() {
        return new SmsSender();
    }
}
