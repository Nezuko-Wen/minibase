package com.minibase.exec.logexec;

import com.minibase.exec.KeyValue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

/**
 * 队列异步写入，实际写入为单线程，单日志文件顺序读取
 *
 * @author zouzhiwen
 * @date 2022/1/5 10:16
 */
public class MiniWalLogQueue extends MiniWalLog {
    private int rounds = 0;
    private volatile boolean flushing = false;
    private FileWriter writer;
    private final ConcurrentLinkedQueue<KeyValue> lazyWriteQueue = new ConcurrentLinkedQueue<>();
    private volatile int state = 0;
    //性能测试闸门
    private CountDownLatch completeSign;

    public MiniWalLogQueue(CountDownLatch countDownLatch) {
        completeSign = countDownLatch;
    }

    public MiniWalLogQueue() {
    }

    public void doWrite() {
        if (flushing) {
            System.out.println("日志清理中......");
            return;
        }
        try {
            //测试中，工作时间太短的话，效果不明显，这里休眠一下
//            Thread.sleep(1);
            KeyValue keyValue = lazyWriteQueue.poll();
            Objects.requireNonNull(keyValue);
            if (writer == null) {
                String filePath = Conf.LOG_PATH + rounds + "-.wal";
                writer = new FileWriter(new File(filePath));
            }
            //写入日志文件
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized KeyValue write(KeyValue keyValue) {
        lazyWriteQueue.offer(keyValue);
        if (state == 0) {
            state = 1;
            new Thread(new WriteRun(this)).start();
        }
        return keyValue;
    }

    public synchronized void clear() {
        if (flushing) return;
        toFlush();
        System.out.println("开始清理.......");
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            writer = null;
        }
        File fileD = new File(Conf.LOG_PATH);
        File[] files = fileD.listFiles();
        if (!Objects.isNull(files)) {
            for (File file : files) {
                if (file.getPath().contains(".wal")) {
                    file.renameTo(new File(Conf.LOG_BAK_PATH + file.getName()));
                }
            }
        }
        flushing = false;
    }

    public void toFlush() {
        flushing = true;
        rounds++;
    }

    static class WriteRun implements Runnable {
        MiniWalLogQueue miniWalLogQueue;

        public WriteRun(MiniWalLogQueue miniWalLogQueue) {
            this.miniWalLogQueue = miniWalLogQueue;
        }

        @Override
        public void run() {
//            while (!miniWalLogQueue.lazyWriteQueue.isEmpty() || miniWalLogQueue.dataIndex.get() < 800000) {
            while (!miniWalLogQueue.lazyWriteQueue.isEmpty()) {
                synchronized (miniWalLogQueue.lazyWriteQueue) {
                    while (!miniWalLogQueue.lazyWriteQueue.isEmpty()) {
                        miniWalLogQueue.doWrite();
                    }
                }
            }
//            miniWalLogQueue.completeSign.countDown();
        }
    }
}
