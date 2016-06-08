package com.kong.design.create.factory.staticFactory;

import com.kong.design.create.factory.general.MailSender;
import com.kong.design.create.factory.general.SMSSender;
import com.kong.design.create.factory.general.Sender;

/**
 * Created by kong on 2016/4/26.
 */
public class SenderFactory {
    public static Sender produceSmsSender() {
        return new SMSSender();
    }

    public static Sender produceMailSender() {
        return new MailSender();
    }
}
