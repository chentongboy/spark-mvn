package com.kong.jvm;

import java.util.ArrayList;
import java.util.List;

/**
 * 演示Constant内存溢出
 * Created by kong on 2016/4/3 0003.
 */
public class HelloConstantOutOfMemory {
    public static void main(String[] args) {
        try {

            List<String> stringList = new ArrayList<String>();

            int item = 0;

            while(true){

                stringList.add(String.valueOf(item++).intern());

            }

        } catch (Exception e) {

            e.printStackTrace();

            //throw e;

        }

    }
}
