package com.kong.design.active.iterator;

/**
 * 迭代器模式就是顺序访问聚集中的对象，一般来说，集合中非常常见，如果对集合类比较熟悉的话，理解本模式会十分轻松。这句话包含两层意思：
 * 一是需要遍历的对象，即聚集对象，二是迭代器对象，用于对聚集对象进行遍历访问。
 * Created by kong on 2016/4/26.
 */
public interface Collection {
    public Iterator iterator();

    /*取得集合元素*/
    public Object get(int i);

    /*取得集合大小*/
    public int size();
}
