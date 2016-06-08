package com.kong.design.active.visitor;

/**
 * Created by kong on 2016/4/26.
 */
public class MySubject implements Subject {
    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String getSubject() {
        return "kong";
    }
}
