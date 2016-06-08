package com.kong.spark.rdd;

import java.io.Serializable;

import scala.math.Ordered;

/**
 * 自定义二次排序的key
 */
public class SecondarySort implements Ordered<SecondarySort>, Serializable {
    private int first;
    private int seconds;

    public SecondarySort(int first, int seconds) {
        this.first = first;
        this.seconds = seconds;
    }

    @Override
    public int compare(SecondarySort that) {
        if (this.first - that.getFirst() != 0)
            return this.first - that.getFirst();
        else
            return this.seconds - that.getSeconds();
    }

    @Override
    public boolean $less(SecondarySort that) {
        if (this.first < that.getFirst())
            return true;
        else if (this.first == that.getFirst() && this.seconds < that.getSeconds())
            return true;
        return false;
    }

    public boolean $greater(SecondarySort other) {
        if (this.first > other.first) {
            return true;
        } else if (this.first == other.getFirst() && this.seconds > other.seconds)
            return true;
        return false;
    }

    @Override
    public boolean $less$eq(SecondarySort that) {
        if (this.$less(that))
            return true;
        else if (this.first == that.getFirst() && this.seconds == that.getSeconds())
            return true;
        return false;
    }

    @Override
    public boolean $greater$eq(SecondarySort that) {
        if (this.$greater(that))
            return true;
        else if (this.first == that.getFirst() && this.seconds == that.getSeconds())
            return true;
        return false;
    }

    @Override
    public int compareTo(SecondarySort that) {
        if (this.first - that.getFirst() != 0)
            return this.first - that.getFirst();
        else
            return this.seconds - that.getSeconds();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSeconds() {
        return seconds;
    }

    public void setSeconds(int seconds) {
        this.seconds = seconds;
    }
}