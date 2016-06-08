package com.kong.design.construct.bridge;

/**
 * Created by kong on 2016/4/21.
 */
public class BridgeTest {
    public static void main(String[] args) {
        Bridge bridge = new MyBridge();
        /*调用第一个对象*/
        Sourceable source1 = new SourceSub1();
        bridge.setSourceable(source1);
        bridge.method();

        /*调用第二个对象*/
        Sourceable source2 = new SourceSub2();
        bridge.setSourceable(source2);
        bridge.method();
    }

}
