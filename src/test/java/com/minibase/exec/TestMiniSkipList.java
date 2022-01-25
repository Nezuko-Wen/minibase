package com.minibase.exec;

import com.minibase.exec.logexec.MiniSkipList;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
 * @author zouzhiwen
 * @date 2022/1/24 15:29
 */
public class TestMiniSkipList {

    /**
     * 多线程put正确性测试
     */
    @Test
    public void putMulti() throws InterruptedException {
        multiThreadTest();
    }

    /**
     * 单线程put正确性测试
     */
    @Test
    public void putSingle() {
        singleThreadTest();
    }

    /**
     * get性能测试
     */
    @Test
    public void getPerformanceTest() {
        MiniSkipList<Integer, Integer> miniSkipList = new MiniSkipList<>();
        for (int i = 0; i <100000; i ++) {
            miniSkipList.put(i,i,false);
        }
        long nodeListStart = System.currentTimeMillis();
        //链表get
        for (int i = 1; i < 99999;i ++) {
            miniSkipList.bootomGet(i);
        }
        long middle = System.currentTimeMillis();
        System.out.println(middle - nodeListStart);
        //跳表get
        for (int i = 1; i < 99999;i ++) {
            miniSkipList.get(i);
        }
        System.out.println(System.currentTimeMillis() - middle);
    }

    void singleThreadTest() {
        MiniSkipList<Integer, Integer> miniSkipList = new MiniSkipList<>();
        for (int i = 0; i<50; i++) {
            miniSkipList.put(i, i, false);
        }
        miniSkipList.print();
    }

    void multiThreadTest() throws InterruptedException {
        int interval = 100;
        MiniSkipList<Integer, Integer> miniSkipList = new MiniSkipList<>();
        CountDownLatch downLatch = new CountDownLatch(interval);
        for (int i = 1; i <= interval; i ++) {
            new Thread(new MultiPutThread(downLatch, miniSkipList, i, interval)).start();
        }
        downLatch.await();
        miniSkipList.print();
    }

    class MultiPutThread implements Runnable {

        private CountDownLatch countDownLatch;
        private MiniSkipList<Integer, Integer> miniSkipList;
        private int start;
        private int interval;

        public MultiPutThread(CountDownLatch countDownLatch, MiniSkipList<Integer, Integer> miniSkipList, int start, int intervel) {
            this.countDownLatch = countDownLatch;
            this.miniSkipList = miniSkipList;
            this.start = start;
            this.interval = intervel;
        }

        @Override
        public void run() {
            for (int i = start; i <= 1000;i = i + interval) {
                miniSkipList.put(i,i,false);
            }
            countDownLatch.countDown();
        }
    }
}
