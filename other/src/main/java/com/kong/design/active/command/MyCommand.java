package com.kong.design.active.command;

/**
 * Created by kong on 2016/4/26.
 */
public class MyCommand implements Command {

    private Receiver receiver;

    public MyCommand(Receiver receiver) {
        this.receiver = receiver;
    }

    @Override
    public void exe() {
        receiver.action();
    }
}
