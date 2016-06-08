package com.kong.design.create.factory.general;

/**
 * Created by kong on 2016/4/26.
 */
public class SenderFactoryTest {
    public static void main(String[] args) {
        SenderFactory senderFactory = new SenderFactory();
        Sender sender = senderFactory.produce("sms");
        sender.send();
    }
}
