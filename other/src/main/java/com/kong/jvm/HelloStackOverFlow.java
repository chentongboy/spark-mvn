package com.kong.jvm;

/**
 * 演示栈溢出
 * Created by kong on 2016/4/3 0003.
 */
public class HelloStackOverFlow {
    private int count;

    public void count() {
        count++;
        count();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("HelloStackOverFlow");

        HelloStackOverFlow helloStackOverFlow = new HelloStackOverFlow();

        try {
            helloStackOverFlow.count();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
