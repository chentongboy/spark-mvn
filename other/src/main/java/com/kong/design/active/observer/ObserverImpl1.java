package com.kong.design.active.observer;

/**
 * Created by kong on 2016/4/26.
 */
public class ObserverImpl1 extends Observer {
    @Override
    public void update() {
        System.out.println("Observer1");
    }
}
