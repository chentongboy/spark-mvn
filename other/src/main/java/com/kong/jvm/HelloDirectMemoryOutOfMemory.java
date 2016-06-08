package com.kong.jvm;

import java.nio.ByteBuffer;

/**
 * 演示Direct内存溢出错误
 * Created by kong on 2016/4/3 0003.
 */
public class HelloDirectMemoryOutOfMemory {
    private static final int ONE_GB = 1024*1024*1024;

    private static int count = 1;

    public static void main(String[] args) {
        try {

            while (true) {

                ByteBuffer buffer = ByteBuffer.allocateDirect(ONE_GB);

                count++;

            }

        } catch (Exception e) {

            System.out.println("Exception:instance created "+count);

            e.printStackTrace();

        } catch (Error e) {

            System.out.println("Error:instance created "+count);

            e.printStackTrace();

        }
    }
}
