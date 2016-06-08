package com.kong.design.active.mediator;

/**
 * Created by kong on 2016/4/26.
 */
public class MyMediator implements Mediator {

    private User1 user1;
    private User2 user2;

    public User1 getUser1() {
        return user1;
    }

    public User2 getUser2() {
        return user2;
    }

    @Override
    public void createMediator() {
        user1 = new User1(this);
        user2 = new User2(this);
    }

    @Override
    public void workerAll() {
        user1.worker();
        user2.worker();
    }
}
