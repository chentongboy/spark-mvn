package com.kong.design.create.factory.staticFactory;

import com.kong.design.create.factory.general.Sender;

/**
 * Created by kong on 2016/4/26.
 */
public class StaticTest {
    public static void main(String[] args) {
        Sender sender = SenderFactory.produceMailSender();
        sender.send();
    }
}
