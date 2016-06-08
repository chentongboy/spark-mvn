package com.kong.design.active.responsibility;

/**
 * Created by kong on 2016/4/26.
 */
public class MyHandler extends AbstractHandler implements Handler {

    private String name;

    public MyHandler(String name) {
        this.name = name;
    }

    @Override
    public void operator() {
        System.out.println(name+":deal!");
        if (getHandler()!=null){
            getHandler().operator();
        }
    }
}
