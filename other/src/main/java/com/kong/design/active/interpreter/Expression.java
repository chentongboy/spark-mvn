package com.kong.design.active.interpreter;

/**
 * 解释器模式是我们暂时的最后一讲，一般主要应用在OOP开发中的编译器的开发中，所以适用面比较窄。
 * Created by kong on 2016/4/26.
 */
public interface Expression {
    int interpret(Context context);
}
