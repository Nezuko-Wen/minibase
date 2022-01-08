package com.minibase.exec.logexec;

/**
 * @author zouzhiwen
 * @date 2022/1/5 10:47
 */
public enum Op {
    Put(0),
    Delete(1);

    private int value;

    Op(int value) {
        this.value = value;
    }


    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
