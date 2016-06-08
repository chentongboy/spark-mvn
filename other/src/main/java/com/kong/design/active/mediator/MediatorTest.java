package com.kong.design.active.mediator;

/**
 * Created by kong on 2016/4/26.
 */
public class MediatorTest {
    public static void main(String[] args) {
        Mediator mediator = new MyMediator();
        mediator.createMediator();
        mediator.workerAll();
    }
}
