package com.kong.design.active.command;

/**
 * Created by kong on 2016/4/26.
 */
public class Invoker {
    private Command command;

    public Invoker(Command command) {
        this.command = command;
    }

    public void action() {
        command.exe();
    }
}
