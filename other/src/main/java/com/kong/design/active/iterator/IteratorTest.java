package com.kong.design.active.iterator;

/**
 * Created by kong on 2016/4/26.
 */
public class IteratorTest {
    public static void main(String[] args) {
        Collection collection = new MyCollection();
        Iterator it = collection.iterator();

        while (it.hasNext()) {
            System.out.println(it.next());
        }
    }
}
