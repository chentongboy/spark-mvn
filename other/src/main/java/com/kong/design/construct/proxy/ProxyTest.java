package com.kong.design.construct.proxy;

/**
 * Created by kong on 2016/4/26.
 */
public class ProxyTest {
    public static void main(String[] args) {
        Sourceable source = new Proxy();
        source.method();
    }
}
