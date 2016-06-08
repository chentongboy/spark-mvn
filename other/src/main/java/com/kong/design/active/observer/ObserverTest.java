package com.kong.design.active.observer;

/**
 * Created by kong on 2016/4/26.
 */
public class ObserverTest {
    public static void main(String[] args) {
        Subject sub = new MySubject();
        sub.add(new ObserverImpl1());
        sub.add(new ObserverImpl2());

        sub.operation();
    }
}
