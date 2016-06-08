package com.kong.design.construct.adapter;

/**
 * 有一个Source类，拥有一个方法，待适配，目标接口时Targetable，通过Adapter类，将Source的功能扩展到Targetable里
 * 继承方式，也叫类适配器
 * Created by kong on 2016/4/21.
 */
public class Adapter extends Source implements Targetable {

    @Override
    public void method2() {
        System.out.println("method2 ...");
    }
}
