package com.kong.design.create.factory.abstractFactory;

/**
 * 发及时信息，则只需做一个实现类，实现Sender接口，同时做一个工厂类，实现Provider接口，就OK了，无需去改动现成的代码。这样做，拓展性较好！
 * Created by kong on 2016/4/26.
 */
public class AbstractTest {
    public static void main(String[] args) {
        Provider provider = new MailFactory();
        Sender sender = provider.produce();
        sender.send();
    }
}
