package com.minibase.exec.logexec;

import com.minibase.exec.Bytes;
import com.minibase.exec.KeyValue;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;

/**
 * @author zouzhiwen
 * @date 2022/1/5 11:00
 */
public class LogTest {
    public static int fileNum = 5000;
    public static int threadNum = 16;
    public static MiniSkipList miniSkipList = new MiniSkipList();
    public static void main(String[] args) throws InterruptedException {

//        long start = System.currentTimeMillis();
//        testQueue();
//        long end = System.currentTimeMillis();
//        System.out.println((end - start));
//        while (true) {
//
//        }

//
        new Thread(()->{
            try {
                Thread.sleep(1000);
                miniSkipList = null;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
        Thread thread = new Thread(() -> {
            if (miniSkipList != null) {
                try {
                    Thread.sleep(10000);
                    System.out.println(miniSkipList.getState());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        thread.start();
        thread.join();

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
