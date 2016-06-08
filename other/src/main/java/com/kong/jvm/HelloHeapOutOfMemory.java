package com.kong.jvm;

import java.util.ArrayList;
import java.util.List;

/**
 * 演示堆的溢出实战
 * Created by kong on 2016/4/3 0003.
 */

class Person{}

public class HelloHeapOutOfMemory {
    public static void main(String[] args){
        System.out.println("HelloHeapOutOfMemory");
        List<Person> persons = new ArrayList<Person>();
        int count = 0;
        while (true){
            persons.add(new Person());
            System.out.println("instance:"+(++count));
        }
    }
}
