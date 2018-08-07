package cn.xpleaf.bigdata.spark.java.core.domain;

import scala.Serializable;

public class SecondarySort implements Comparable<SecondarySort>, Serializable {
    private int first;
    private int second;

    public SecondarySort(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    @Override
    public int compareTo(SecondarySort that) {
        int ret = this.getFirst()  - that.getFirst();
        if(ret == 0) {
            ret = that.getSecond() - this.getSecond();
        }
        return ret;
    }

    @Override
    public String toString() {
        return this.first + " " + this.second;
    }
}
