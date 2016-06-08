package com.kong.design.create.singleton;

import java.util.Vector;

/**
 * 影子模式
 * 为单例对象的属性同步更新
 * Created by kong on 2016/4/26.
 */
public class SingletonTest {

    private static SingletonTest instance = null;
    private Vector properties = null;

    public Vector getProperties() {
        return properties;
    }

    private SingletonTest() {
    }

    private static synchronized void syncInit() {
        if (instance == null) {
            instance = new SingletonTest();
        }
    }

    public static SingletonTest getInstance() {
        if (instance == null) {
            syncInit();
        }
        return instance;
    }

    public void updateProperties() {
        SingletonTest shadow = new SingletonTest();
        properties = shadow.getProperties();
    }
}
