package com.kong.design.create.factory.more;

import com.kong.design.create.factory.general.MailSender;
import com.kong.design.create.factory.general.SMSSender;
import com.kong.design.create.factory.general.Sender;

/**
 * Created by kong on 2016/4/26.
 */
public class SenderFactory {
    public Sender produceSmsSender() {
        return new SMSSender();
    }

    public Sender produceMailSender() {
        return new MailSender();
    }
}
