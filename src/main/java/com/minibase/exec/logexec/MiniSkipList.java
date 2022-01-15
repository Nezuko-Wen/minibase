package com.minibase.exec.logexec;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.Objects;

/**
 * @author zouzhiwen
 * @date 2022/1/6 14:23
 */
public class MiniSkipList<K, V> {

    private HeadIndex<K, V> head;

    private int state;

    public MiniSkipList() {
        this.state = 1;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    V doPut(K key, V value, boolean onlyIfAbsent) {
        Objects.requireNonNull(key);
        out:
        for (; ; ) {
            //通过上层索引找到最底层的节点位置，及插入节点的前继节点
            for (Node<K, V> base = findPredecessor(key), n = base.next; ; ) {
                if (n != null) {
                    Object v; int c;
                    if (n != base.next) { //读不一致
                        break;
                    }
                    if ((v = n.value) == null) { //n节点已被删
                        break;
                    }
                    if (base.value == null) { //base节点已被删
                        break;
                    }
                    if ((c = cm(key, n.key)) > 0) { //插入节点大于next节点
                        base = n;
                        n = n.next;
                        continue ;
                    }
                    if (c == 0) { //插入节点等于next节点，替换值
                        if (onlyIfAbsent || n.casValue(v, value)) {
                            @SuppressWarnings("unchecked") V vv = (V) v;
                            return vv;
                        }
                    }
                    //插入节点小于next节点，说明前继节点的key与插入key相等，继续向下运行
                }
                Node<K, V> z = new Node<>(key, value, n);
                if (!base.casNext(n, z)) {
                    break;
                }
                break out;
            }
        }
        return null;
    }

    private Node<K, V> findPredecessor(Object key) {
        Objects.requireNonNull(key);
        for (; ; ) {
            for (Index<K, V> h = head, r = h.right, d; ; ) {
                if (r != null) {
                    K rk = r.node.key;
                    if (cm(key, rk) > 0) {
                        h = r;
                        r = r.right;
                        continue;
                    }
                }
                if ((d = h.down) == null) {
                    return h.node;
                }
                h = d;
                r = d.right;
                break;
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    int cm(Object key, Object rk) {
        return ((Comparable) key).compareTo(rk);
    }

    static class HeadIndex<K, V> extends Index<K, V> {
        final int level;

        HeadIndex(Node<K, V> node, Index<K, V> down, Index<K, V> right, int level) {
            super(node, down, right);
            this.level = level;
        }
    }

    static class Index<K, V> {
        final Node<K, V> node;
        volatile Index<K, V> down;
        volatile Index<K, V> right;

        Index(Node<K, V> node, Index<K, V> down, Index<K, V> right) {
            this.node = node;
            this.down = down;
            this.right = right;
        }

        private static final sun.misc.Unsafe UNSAFE;
        private static final long rightOffset;

        static {
            try {
                UNSAFE = reflectUnsafe();
                assert UNSAFE != null;
                Class<?> k = Index.class;
                rightOffset = UNSAFE.objectFieldOffset
                        (k.getDeclaredField("right"));
            } catch (Exception e) {
                throw new Error(e);
            }
        }

    }

    static class Node<K, V> {
        final K key;
        volatile Object value;
        volatile Node<K, V> next;

        Node(K key, Object value, Node<K, V> next) {
            this.key = key;
            this.value = value;
            this.next = next;
        }

        private static final sun.misc.Unsafe UNSAFE;
        private static final long valueOffset;
        private static final long nextOffset;

        static {
            try {
                UNSAFE = reflectUnsafe();
                assert UNSAFE != null;
                Class<?> k = Node.class;
                valueOffset = UNSAFE.objectFieldOffset
                        (k.getDeclaredField("value"));
                nextOffset = UNSAFE.objectFieldOffset
                        (k.getDeclaredField("next"));
            } catch (Exception e) {
                throw new Error(e);
            }
        }

        boolean casNext(Object expect, Object update) {
            return UNSAFE.compareAndSwapObject(this, nextOffset, expect, update);
        }

        boolean casValue(Object expect, Object update) {
            return UNSAFE.compareAndSwapObject(this, valueOffset, expect, update);
        }
    }

    public static Unsafe reflectUnsafe() {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            return (Unsafe) field.get(null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }
}
