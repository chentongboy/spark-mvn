package com.kong.design.create.factory.more;

import com.kong.design.create.factory.general.Sender;

/**
 * Created by kong on 2016/4/26.
 */
public class MoreTest {
    public static void main(String[] args) {
        SenderFactory factory = new SenderFactory();
        Sender sender = factory.produceMailSender();
        sender.send();
    }
}
