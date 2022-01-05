package com.exec.minibase.wal;

import com.exec.minibase.meta.KeyValue;
import com.exec.minibase.meta.Op;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author zouzhiwen
 * @date 2022/1/5 18:04
 */
public class MiniWalLogFiles {
    private final int fileSize;
    private final AtomicInteger writeIndex = new AtomicInteger();
    private final AtomicInteger dataIndex = new AtomicInteger();
    private int rounds = 0;
    private volatile boolean flushing = false;
    private final FileWriter[] writers;
    private final ConcurrentLinkedQueue<KeyValue> lazyWriteQueue = new ConcurrentLinkedQueue<>();
    private final ReentrantLock[] writeLock;

    static final int MAXIMUM_FILESIZE = 1 << 30;
    static final int DEFAULT_FILESIZE = 1 << 4;

    public MiniWalLogFiles() {
        this.fileSize = DEFAULT_FILESIZE;
        writeLock = new ReentrantLock[this.fileSize];
        for (int i = 0; i < this.fileSize; i++) {
            writeLock[i] = new ReentrantLock();
        }
        writers = new FileWriter[this.fileSize];
    }

    public MiniWalLogFiles(int fileSize) {
        this.fileSize = fileSize(fileSize);
        writeLock = new ReentrantLock[this.fileSize];
        for (int i = 0; i < this.fileSize; i++) {
            writeLock[i] = new ReentrantLock(true);
        }
        writers = new FileWriter[this.fileSize];
    }

    public AtomicInteger getDataIndex() {
        return dataIndex;
    }

    public int fileSize(int fileSize) {
        int n = fileSize - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= MAXIMUM_FILESIZE) ? MAXIMUM_FILESIZE : n + 1;
    }

    public void doWrite(KeyValue keyValue) throws InterruptedException {
        int seqId;
        int fileIndex;
        synchronized (writeLock) {
            //进入分段锁之前保证顺序
            boolean lazyWrite = false;
            while (flushing) {
                System.out.println("日志清理中......");
                if (!lazyWrite) {
                    lazyWrite = true;
                    lazyWriteQueue.add(keyValue);
                }
            }
            if (lazyWrite) {
                toLazyWrite();
                return;
            }
            seqId = writeIndex.incrementAndGet() - 1;
            fileIndex = (seqId & (fileSize - 1));
            writeLock[fileIndex].lock();
        }
        try {
            FileWriter writer;
            try {
                keyValue.setSeqId(seqId);
                writer = writers[fileIndex];
                if (writer == null) {
                    String filePath = "d:\\minibase\\wal\\" + rounds + "-" + fileIndex + ".wal";
                    writer = new FileWriter(new File(filePath));
                    writers[fileIndex] = writer;
                }
                String key = new String(keyValue.getKey());
                String value = new String(keyValue.getValue());
                Op op = keyValue.getOp();
                String line = String.format("%s|%s|%s|%s|%s\n", Thread.currentThread().getName(), key, value, seqId, op.getValue());
                writer.append(line);
                writer.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } finally {
            writeLock[fileIndex].unlock();
        }
    }

    private void toLazyWrite() throws InterruptedException {
        synchronized (lazyWriteQueue) {
            while (lazyWriteQueue.size() > 0) {
                KeyValue value = lazyWriteQueue.poll();
                doWrite(value);
            }
        }
    }

    public synchronized void clear() throws IOException, InterruptedException {
        if (flushing) return;
        toFlush();
        System.out.println("开始清理.......");
        for (int i = 0; i < writers.length; i++) {
            if (writers[i] != null) {
                writers[i].close();
                writers[i] = null;
            }
        }
        File fileD = new File("d:\\minibase\\wal");
        File[] files = fileD.listFiles();
        if (!Objects.isNull(files)) {
            for (File file : files) {
                if (file.getPath().contains(".wal")) {
                    file.renameTo(new File("d:\\minibase\\walbak\\" + file.getName()));
                }
            }
        }
        Thread.sleep(10000);
        flushing = false;
    }

    public void toFlush() {
        flushing = true;
        writeIndex.set(0);
        rounds++;
    }


}
