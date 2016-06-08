package com.kong.design.construct.proxy;

/**
 * Created by kong on 2016/4/26.
 */
public class Proxy implements Sourceable {

    private Source source;

    public Proxy() {
    }

    public Proxy(Source source) {
        this.source = source;
    }

    @Override
    public void method() {
        before();
        source.method();
        after();
    }

    private void before(){
        System.out.println("before proxy");
    }

    private void after(){
        System.out.println("after proxy");
    }
}
