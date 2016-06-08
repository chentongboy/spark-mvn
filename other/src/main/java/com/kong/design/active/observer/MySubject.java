package com.kong.design.active.observer;

/**
 * Created by kong on 2016/4/26.
 */
public class MySubject extends AbstractSubject {
    @Override
    public void operation() {
        System.out.println("update self!");
        notifyObservers();
    }
}
