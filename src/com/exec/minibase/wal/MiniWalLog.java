package com.exec.minibase.wal;

import com.exec.minibase.meta.KeyValue;
import com.exec.minibase.meta.Op;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zouzhiwen
 * @date 2022/1/5 10:16
 */
public class MiniWalLog {
    private final AtomicInteger writeIndex = new AtomicInteger();
    private int rounds = 0;
    private volatile boolean flushing = false;
    private volatile boolean writing = false;
    private FileWriter writer;
    private final ConcurrentLinkedQueue<KeyValue> lazyWriteQueue = new ConcurrentLinkedQueue<>();
    private volatile int state = 0;

    public MiniWalLog() {

    }

    public void doWrite() {
        if (flushing) {
            System.out.println("日志清理中......");
            return;
        }
        writing = true;
        try {
            KeyValue keyValue = lazyWriteQueue.poll();
            int seqId = writeIndex.incrementAndGet() - 1;
            Objects.requireNonNull(keyValue).setSeqId(seqId);
            if (writer == null) {
                String filePath = "d:\\minibase\\wal\\" + rounds + "-.wal";
                writer = new FileWriter(new File(filePath));
            }
            String key = new String(keyValue.getKey());
            String value = new String(keyValue.getValue());
            Op op = keyValue.getOp();
            String line = String.format("%s|%s|%s|%s|%s\n", Thread.currentThread().getName(), key, value, seqId, op.getValue());
            writer.append(line);
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            writing = false;
        }
    }

    public void write(KeyValue keyValue) {
        lazyWriteQueue.offer(keyValue);
        if (state == 0) {
            synchronized (lazyWriteQueue) {
                if (state == 0) {
                    new Thread(new WriteRun(this)).start();
                }
            }
        }
    }

    public synchronized void clear() throws IOException, InterruptedException {
        if (flushing) return;
        while (writing) {
            System.out.println("写入未结束........");
        }
        toFlush();
        System.out.println("开始清理.......");
        if (writer != null) {
            writer.close();
            writer = null;
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
        rounds++;
    }

    static class WriteRun implements Runnable {
        MiniWalLog miniWalLog;

        public WriteRun(MiniWalLog miniWalLog) {
            this.miniWalLog = miniWalLog;
        }

        @Override
        public void run() {
            miniWalLog.state = 1;
            while (true) {
                synchronized (miniWalLog.lazyWriteQueue) {
                    while (!miniWalLog.lazyWriteQueue.isEmpty()) {
                        miniWalLog.doWrite();
                    }
                }
            }
        }
    }
}
