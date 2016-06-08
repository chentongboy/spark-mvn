package com.kong.design.construct.bridge;

/**
 * Created by kong on 2016/4/21.
 */
public class MyBridge extends Bridge {
    @Override
    public void method() {
        getSourceable().method();
    }
}
