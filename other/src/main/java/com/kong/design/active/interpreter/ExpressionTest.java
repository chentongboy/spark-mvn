package com.kong.design.active.interpreter;

/**
 * Created by kong on 2016/4/26.
 */
public class ExpressionTest {
    public static void main(String[] args) {

        // 计算9+2-8的值
        int result = new Minus().interpret((new Context(new Plus()
                .interpret(new Context(9, 2)), 8)));
        System.out.println(result);
    }
}
