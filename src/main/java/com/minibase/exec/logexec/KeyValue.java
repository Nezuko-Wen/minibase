package com.minibase.exec.logexec;

/**
 * @author zouzhiwen
 * @date 2022/1/5 10:44
 */
public class KeyValue {
    private byte[] key;
    private byte[] value;
    private int seqId;
    private Op op;
    private String threadName;

    public KeyValue(String threadName, byte[] key, byte[] value, Op op) {
        this.key = key;
        this.value = value;
        this.op = op;
        this.threadName = threadName;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public int getSeqId() {
        return seqId;
    }

    public void setSeqId(int seqId) {
        this.seqId = seqId;
    }

    public Op getOp() {
        return op;
    }

    public void setOp(Op op) {
        this.op = op;
    }

    public String getThreadName() {
        return threadName;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }
}
