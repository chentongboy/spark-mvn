package com.kong.design.active.responsibility;

/**
 * Created by kong on 2016/4/26.
 */
public abstract class AbstractHandler {
    private Handler handler;

    public Handler getHandler() {
        return handler;
    }

    public void setHandler(Handler handler) {
        this.handler = handler;
    }
}
