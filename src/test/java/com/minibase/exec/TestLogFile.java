package com.minibase.exec;

import com.minibase.exec.logexec.*;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
 * @author zouzhiwen
 * @date 2022/1/25 17:38
 */
public class TestLogFile {

    public static int fileNum = 5000;
    public static int threadNum = 16;

    @Test
    public void testQueue() {
        long start = System.currentTimeMillis();
        queue();
        long end = System.currentTimeMillis();
        System.out.println((end - start));
    }

    @Test
    public void testFiles() throws InterruptedException {
        long start = System.currentTimeMillis();
        files();
        long end = System.currentTimeMillis();
        System.out.println((end - start));
    }

    @Test
    public void testFilesClear() throws InterruptedException {
        filesClear();
    }

    void filesClear() throws InterruptedException {
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

    void files() throws InterruptedException {
        MiniWalLog miniWalLogFiles = new MiniWalLogFiles(threadNum);
        CountDownLatch fileDownLaunch = new CountDownLatch(threadNum);
        for (int i = 0; i < threadNum; i ++) {
            Thread thread = new Thread(new WriteRunFiles(miniWalLogFiles, fileDownLaunch));
            thread.start();
        }
        fileDownLaunch.await();
    }

    void queue() {
        MiniWalLog miniWalLogQueue = new MiniWalLogQueue();
        for (int i = 0; i < threadNum; i ++) {
            Thread thread = new Thread(new WriteRunQueue(miniWalLogQueue));
            thread.start();
        }
    }

    class WriteRunFiles implements Runnable {

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

    class WriteRunQueue implements Runnable {

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
    class ClearRun implements Runnable {

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
