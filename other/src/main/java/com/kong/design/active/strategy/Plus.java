package com.kong.design.active.strategy;

/**
 * Created by kong on 2016/4/22.
 */
public class Plus extends AbstractCalculator implements ICalculator {

    @Override
    public int calculate(String exp) {
        int[] arrayInt = split(exp, "\\+");
        return arrayInt[0] + arrayInt[1];
    }
}
