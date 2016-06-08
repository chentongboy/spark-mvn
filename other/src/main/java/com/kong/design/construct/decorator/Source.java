package com.kong.design.construct.decorator;

/**
 * Created by kong on 2016/4/21.
 */
public class Source implements Sourceable {

    @Override
    public void method() {
        System.out.println("source: method ...");
    }
}
