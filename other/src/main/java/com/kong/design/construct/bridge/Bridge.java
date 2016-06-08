package com.kong.design.construct.bridge;

/**
 * Created by kong on 2016/4/21.
 */
public abstract class Bridge {
    private Sourceable sourceable;

    public void method(){
        sourceable.method();
    }

    public Sourceable getSourceable() {
        return sourceable;
    }

    public void setSourceable(Sourceable sourceable) {
        this.sourceable = sourceable;
    }
}
