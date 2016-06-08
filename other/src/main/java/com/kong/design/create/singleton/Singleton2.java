package com.kong.design.create.singleton;

/**
 * 1、某些类创建比较频繁，对于一些大型的对象，这是一笔很大的系统开销。
 * 2、省去了new操作符，降低了系统内存的使用频率，减轻GC压力。
 * 3、有些类如交易所的核心交易引擎，控制着交易流程，如果该类可以创建多个的话，系统完全乱了。
 * （比如一个军队出现了多个司令员同时指挥，肯定会乱成一团），所以只有使用单例模式，才能保证核心交易服务器独立控制整个流程。
 * Created by kong on 2016/4/26.
 */
public class Singleton2 {
    /* 私有构造方法，防止被实例化 */
    private Singleton2() {
    }

    /** 此处使用一个内部类来维护单例
    * 单例模式使用内部类来维护单例的实现，JVM内部的机制能够保证当一个类被加载的时候，这个类的加载过程是线程互斥的。
    * 这样当我们第一次调用getInstance的时候，JVM能够帮我们保证instance只被创建一次，并且会保证把赋值给instance的内存初始化完毕，
    * 这样我们就不用担心Singleton的问题。同时该方法也只会在第一次调用的时候使用互斥机制，这样就解决了低性能问题。
    * */
    private static class SingletonFactory {
        private static Singleton2 instance = new Singleton2();
    }

    /* 获取实例 */
    public static Singleton2 getInstance() {
        return SingletonFactory.instance;
    }

    /* 如果该对象被用于序列化，可以保证对象在序列化前后保持一致 */
    public Object readResolve() {
        return getInstance();
    }
}
