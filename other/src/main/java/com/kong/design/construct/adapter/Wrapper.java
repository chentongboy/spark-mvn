package com.kong.design.construct.adapter;

/**
 * 聚合方式，也叫对象适配器
 * Created by kong on 2016/4/21.
 */
public class Wrapper implements Targetable {

    private Source source;

    public Wrapper(Source source) {
        this.source = source;
    }

    @Override
    public void method1() {
        source.method1();
    }

    @Override
    public void method2() {
        System.out.println("聚合：method ...");
    }
}
