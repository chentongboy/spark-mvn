package com.kong.design.active.mediator;

/**
 * Created by kong on 2016/4/26.
 */
public class User1 extends User {

    public User1(Mediator mediator) {
        super(mediator);
    }

    @Override
    public void worker() {
        System.out.println("user1 worker");
    }
}
