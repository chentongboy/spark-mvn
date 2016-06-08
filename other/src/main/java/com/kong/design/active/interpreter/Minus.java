package com.kong.design.active.interpreter;

/**
 * Created by kong on 2016/4/26.
 */
public class Minus implements Expression {
    @Override
    public int interpret(Context context) {
        return context.getNum1()-context.getNum2();
    }
}
