package com.kong.design.active.visitor;

/**
 * Created by kong on 2016/4/26.
 */
public interface Subject {
    void accept(Visitor visitor);
    String getSubject();
}
