package com.kong.design.construct.decorator;

/**
 * Created by kong on 2016/4/21.
 */
public class Decorator implements Sourceable {

    private Source source;

    public Decorator(Source source) {
        this.source = source;
    }

    @Override
    public void method() {
        System.out.println("进入Decorator....");
        source.method();
        System.out.println("结束Decorator....");
    }
}
