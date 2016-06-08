package com.kong.design.active.mediator;

/**
 * Created by kong on 2016/4/26.
 */
public class User2 extends User {
    public User2(Mediator mediator) {
        super(mediator);
    }

    @Override
    public void worker() {
        System.out.println("user2 work");
    }
}
