package com.minibase.exec.logexec;

import java.util.concurrent.CountDownLatch;

/**
 * @author zouzhiwen
 * @date 2022/1/5 11:00
 */
public class LogTest {
    public static int fileNum = 5000;
    public static int threadNum = 16;
    public static void main(String[] args) throws InterruptedException {

        long start = System.currentTimeMillis();
        testQueue();
        long end = System.currentTimeMillis();
        System.out.println((end - start));
        while (true) {

        }
    }

    static void testFilesClear() throws InterruptedException {
        MiniWalLog miniWalLogFiles = new MiniWalLogFiles(threadNum);
        CountDownLatch fileDownLaunch = new CountDownLatch(threadNum);
        for (int i = 0; i < threadNum; i ++) {
            Thread thread = new Thread(new WriteRunFiles(miniWalLogFiles, fileDownLaunch));
            thread.start();
        }
        Thread thread = new Thread(new ClearRun(miniWalLogFiles));
        thread.start();
        fileDownLaunch.await();
    }

    static void testFiles() throws InterruptedException {
        MiniWalLog miniWalLogFiles = new MiniWalLogFiles(threadNum);
        CountDownLatch fileDownLaunch = new CountDownLatch(threadNum);
        for (int i = 0; i < threadNum; i ++) {
            Thread thread = new Thread(new WriteRunFiles(miniWalLogFiles, fileDownLaunch));
            thread.start();
        }
        fileDownLaunch.await();
    }

    static void testQueue() throws InterruptedException {
        MiniWalLog miniWalLogQueue = new MiniWalLogQueue();
        for (int i = 0; i < threadNum; i ++) {
            Thread thread = new Thread(new WriteRunQueue(miniWalLogQueue));
            thread.start();
        }
    }

    static class WriteRunFiles implements Runnable {

        MiniWalLog miniWalLog;
        CountDownLatch countDownLatch;

        public WriteRunFiles(MiniWalLog miniWalLog, CountDownLatch countDownLatch) {
            this.miniWalLog = miniWalLog;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            for (int i = 0; i < fileNum; i++) {
                miniWalLog.write(new KeyValue(Thread.currentThread().getName(), "key".getBytes(), "value".getBytes(), Op.Put));
            }
            countDownLatch.countDown();
        }
    }

    static class WriteRunQueue implements Runnable {

        MiniWalLog miniWalLogQueue;

        public WriteRunQueue(MiniWalLog miniWalLogQueue) {
            this.miniWalLogQueue = miniWalLogQueue;
        }

        @Override
        public void run() {
            for (int i = 0; i < fileNum; i++) {
                miniWalLogQueue.write(new KeyValue(Thread.currentThread().getName(), "key".getBytes(), "value".getBytes(), Op.Put));
            }
        }
    }
    static class ClearRun implements Runnable {

        MiniWalLog miniWalLogQueue;

        public ClearRun(MiniWalLog miniWalLogQueue) {
            this.miniWalLogQueue = miniWalLogQueue;
        }

        @Override
        public void run() {
            for (int i = 0; i < 10; i ++) {
                miniWalLogQueue.clear();
            }
        }
    }
}
