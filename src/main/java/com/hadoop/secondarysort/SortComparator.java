package com.hadoop.secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class SortComparator extends WritableComparator {

    protected SortComparator() {
        super(Text.class, true);

    }

    @Override
    public int compare(WritableComparable key1, WritableComparable key2) {
        //如果第一个排序字段相同，则需要比较第二个排序字段
        if (Integer.parseInt(key1.toString().split(" ")[0]) == Integer.parseInt(key2.toString().split(" ")[0])) {
            if (Integer.parseInt(key1.toString().split(" ")[1]) > Integer.parseInt(key2.toString().split(" ")[1])) {
                return 1;
            } else if (Integer.parseInt(key1.toString().split(" ")[1]) < Integer.parseInt(key2.toString().split(" ")[1])) {
                return -1;
            } else if (Integer.parseInt(key1.toString().split(" ")[1]) == Integer.parseInt(key2.toString().split(" ")[1])) {
                return 0;
            }
        } else {
            if (Integer.parseInt(key1.toString().split(" ")[0]) > Integer.parseInt(key2.toString().split(" ")[0])) {
                return 1;
            } else if (Integer.parseInt(key1.toString().split(" ")[0]) == Integer.parseInt(key2.toString().split(" ")[0])) {
                return -1;
            }
        }

        return 0;
    }
}
