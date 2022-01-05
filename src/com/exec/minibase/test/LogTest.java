package com.exec.minibase.test;

import com.exec.minibase.meta.KeyValue;
import com.exec.minibase.meta.Op;
import com.exec.minibase.wal.MiniWalLog;
import com.exec.minibase.wal.MiniWalLogFiles;

import javax.swing.plaf.synth.SynthTextAreaUI;
import java.io.IOException;

/**
 * @author zouzhiwen
 * @date 2022/1/5 11:00
 */
public class LogTest {
    public static void main(String[] args) throws InterruptedException {
        MiniWalLogFiles miniWalLog = new MiniWalLogFiles(10);
        long start = System.currentTimeMillis();

        for (int i = 0; i < 16; i ++) {
            Thread thread = new Thread(new WriteRun(miniWalLog));
            thread.start();
        }

        long end = System.currentTimeMillis();

        System.out.println((end - start));

    }

    static class WriteRun implements Runnable {

        MiniWalLogFiles miniWalLog;

        public WriteRun(MiniWalLogFiles miniWalLog) {
            this.miniWalLog = miniWalLog;
        }

        @Override
        public void run() {
            for (int i = 0; i < 2000; i++) {
                try {
                    miniWalLog.doWrite(new KeyValue("key".getBytes(), "value".getBytes(), Op.Put));
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    static class ClearRun implements Runnable {

        MiniWalLog miniWalLog;

        public ClearRun(MiniWalLog miniWalLog) {
            this.miniWalLog = miniWalLog;
        }

        @Override
        public void run() {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                miniWalLog.clear();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}
