package com.kong.design.active.mediator;

/**
 * Created by kong on 2016/4/26.
 */
public abstract class User {
    private Mediator mediator;

    public User(Mediator mediator) {
        this.mediator = mediator;
    }

    public Mediator getMediator() {
        return mediator;
    }

    public abstract void worker();
}
