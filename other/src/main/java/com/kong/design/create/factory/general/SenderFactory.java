package com.kong.design.create.factory.general;

/**
 * Created by kong on 2016/4/26.
 */
public class SenderFactory {

    public Sender produce(String type) {
        if ("mail".equals(type)) {
            return new MailSender();
        } else if ("sms".equals(type)) {
            return new SMSSender();
        } else {
            System.out.println("请输入正确的类型!");
            return null;
        }
    }
}
