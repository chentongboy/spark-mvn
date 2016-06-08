package com.kong.design.active.iterator;

/**
 * Created by kong on 2016/4/26.
 */
public interface Iterator {
    //前移
    Object previous();

    //后移
    Object next();
    boolean hasNext();

    //取得第一个元素
    Object first();
}
