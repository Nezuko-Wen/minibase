package com.minibase.exec.logexec;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 分段锁同步写入，实际写入并发写入，多日志文件轮询读取
 *
 * @author zouzhiwen
 * @date 2022/1/5 18:04
 */
public class MiniWalLogFiles extends MiniWalLog {
    private int fileSize;
    private final AtomicInteger writeIndex = new AtomicInteger();
    private int rounds = 0;
    private volatile boolean flushing = false;
    private FileWriter[] writers;
    private final ConcurrentLinkedQueue<KeyValue> lazyWriteQueue = new ConcurrentLinkedQueue<>();
    private ReentrantLock[] writeLock;

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
            writeLock[i] = new ReentrantLock();
        }
        writers = new FileWriter[this.fileSize];
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

    public KeyValue write(KeyValue keyValue) {
        int fileIndex;
        int seqId;
        synchronized (this) {
            //这里是实际要写日志的地方，应该先到先写，所以在进入分段锁之前要保证顺序
            seqId = dataIndex.incrementAndGet() - 1;
            keyValue.setSeqId(seqId);
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
                return keyValue;
            }
            fileIndex = ((writeIndex.incrementAndGet() - 1) & (fileSize - 1));
            //拿到分段锁后释放
            writeLock[fileIndex].lock();
        }
        try {
            FileWriter writer;
            try {
                //测试中，工作时间太短的话，效果不明显，这里休眠一下
                Thread.sleep(1);
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
                return keyValue;
            } catch (IOException | InterruptedException e) {
                return keyValue;
            }
        } finally {
            writeLock[fileIndex].unlock();
        }
    }

    private void toLazyWrite() {
        synchronized (lazyWriteQueue) {
            if (!lazyWriteQueue.isEmpty()) {
                doLazyWrite();
            }
        }
    }

    private void doLazyWrite() {
        while (!lazyWriteQueue.isEmpty()) {
            KeyValue keyValue = lazyWriteQueue.poll();
            int fileIndex = ((writeIndex.incrementAndGet() - 1) & (fileSize - 1));
            FileWriter writer;
            try {
                //测试中，工作时间太短的话，效果不明显，这里休眠一下
//                Thread.sleep(1);
                writer = writers[fileIndex];
                if (writer == null) {
                    String filePath = "d:\\minibase\\wal\\" + rounds + "-" + fileIndex + ".wal";
                    writer = new FileWriter(new File(filePath));
                    writers[fileIndex] = writer;
                }
                String key = new String(keyValue.getKey());
                String value = new String(keyValue.getValue());
                Op op = keyValue.getOp();
                String line = String.format("%s|%s|%s|%s|%s\n", keyValue.getThreadName(), key, value, keyValue.getSeqId(), op.getValue());
                writer.append(line);
                writer.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public synchronized void clear() {
        if (flushing) return;
        toFlush();
        System.out.println("开始清理.......");
        for (int i = 0; i < writers.length; i++) {
            if (writers[i] != null) {
                try {
                    writers[i].close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
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
        flushing = false;
    }

    public void toFlush() {
        flushing = true;
        writeIndex.set(0);
        rounds++;
    }


}
