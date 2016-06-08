package com.kong.design.construct.proxy;

/**
 * Created by kong on 2016/4/26.
 */
public class Source implements Sourceable {
    @Override
    public void method() {
        System.out.println("source method");
    }
}
