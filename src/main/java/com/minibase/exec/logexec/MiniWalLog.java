package com.minibase.exec.logexec;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class MiniWalLog {
    protected final AtomicInteger dataIndex = new AtomicInteger();

    public abstract KeyValue write(KeyValue keyValue);

    public abstract void clear();
}
