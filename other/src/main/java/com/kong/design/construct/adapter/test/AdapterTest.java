package com.kong.design.construct.adapter.test;

import com.kong.design.construct.adapter.*;

/**
 * Created by kong on 2016/4/21.
 */
public class AdapterTest {
    public static void main(String[] args) {
        //继承方式
        Targetable t = new Adapter();
        t.method1();
        t.method2();
        //聚合方式
        Source source = new Source();
        Targetable target = new Wrapper(source);
        target.method1();
        target.method2();
        //接口适配器
        Sourceable s1 = new SourceSub1();
        Sourceable s2 = new SourceSub2();
        s1.method1();
        s1.method2();
        s2.method1();
        s2.method2();
    }
}
