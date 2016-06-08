package com.kong.design.active.visitor;

/**
 * Created by kong on 2016/4/26.
 */
public class MyVisitor implements Visitor {
    @Override
    public void visit(Subject sub) {
        System.out.println("visit:"+sub.getSubject());
    }
}
