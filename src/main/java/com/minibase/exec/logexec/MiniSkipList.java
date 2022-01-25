package com.minibase.exec.logexec;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author zouzhiwen
 * @date 2022/1/6 14:23
 */
public class MiniSkipList<K, V> {

    private volatile HeadIndex<K, V> head;

    private static final Object BASE_HEADER = new Object();

    public MiniSkipList() {
        head = new HeadIndex<>(new Node<>(null, BASE_HEADER, null), null, null, 1);
    }

    public V put(K key, V value, boolean onlyIfAbsent) {
        Objects.requireNonNull(value);
        return doPut(key, value, onlyIfAbsent);
    }

    public V get(K key) {
        return doGet(key);
    }

    public V bootomGet(K key) {
        Node base = head.node;
        while (base.next != null) {
            if (cm(key, base.next.key) == 0) {
                @SuppressWarnings("unchecked")
                V vv = (V) base.next.value;
                return vv;
            }
            base = base.next;
        }
        return null;
    }

    V doGet(K key) {
        Objects.requireNonNull(key);
        out:
        for (;;) {
            for (Node<K, V> base = findPredecessor(key), n = base.next; ; ) {
                if (n == null) break out;
                Object v;
                int c;
                if (n != base.next) { //读不一致
                    break;
                }
                if ((v = n.value) == null) { //n节点已被删
                    break;
                }
                if (base.value == null || v == n) { //base节点已被删
                    break;
                }
                if ((c = cm(key, n.key)) == 0) { //插入节点大于next节点
                    @SuppressWarnings("unchecked")
                    V vv = (V) n.value;
                    return vv;
                }
                if (c < 0) break out;
                base = n;
                n = n.next;
            }
        }
        return null;
    }

    V doPut(K key, V value, boolean onlyIfAbsent) {
        Objects.requireNonNull(key);
        Node<K, V> z;//插入的节点
        out:
        for (;;) {
            //通过上层索引找到最底层的节点位置，离插入节点最近的左节点
            for (Node<K, V> base = findPredecessor(key), n = base.next; ; ) {
                if (n != null) {
                    Object v;
                    int c;
                    if (n != base.next) { //读不一致
                        break;
                    }
                    if ((v = n.value) == null) { //n节点已被删
                        break;
                    }
                    if (base.value == null || v == n) { //base节点已被删
                        break;
                    }
                    if ((c = cm(key, n.key)) > 0) { //插入节点大于next节点
                        base = n;
                        n = n.next;
                        continue;
                    }
                    if (c == 0) { //插入节点等于next节点，替换值
                        if (onlyIfAbsent || n.casValue(v, value)) {
                            @SuppressWarnings("unchecked") V vv = (V) v;
                            return vv;
                        }
                    }
                    //插入节点位于base节点和n节点之间，向下运行
                }
                //插入节点的前继节点找到
                z = new Node<>(key, value, n);
                if (!base.casNext(n, z)) {
                    break;
                }
                break out;
            }
        }
        //建立索引节点，是否建立和索引层高都随机
        int rd = ThreadLocalRandom.current().nextInt();
        if ((rd & 0x80000001) == 0) {//是否建立索引
            int level = 1, max = head.level;
            while (((rd >>>= 1) & 1) != 0) {
                level++;
            }
            Index<K, V> idx = null;
            Index<K, V> header = head;
            int oldLevel = head.level;
            if (level <= max) {
                for (int i = 1; i <= level; i++)
                    idx = new Index<>(z, idx, null);//在插入节点上方建立索引节点
            }
            else {
                level = max + 1;
                @SuppressWarnings("unchecked")
                Index<K, V>[] idxs = (Index<K, V>[]) new Index<?, ?>[level + 1];
                for (int i = 1; i <= level; i++) {
                    idxs[i] = idx = new Index<>(z, idx, null);//在插入节点上方建立索引节点
                }
                for (;;) {//先建新的head
                    header = head;//cas失败重新读值
                    oldLevel = head.level;//cas失败重新读值
                    if (oldLevel >= level) {
                        break;
                    }
                    Node<K, V> oldBase = head.node;
                    HeadIndex<K,V> newHead = head;
                    for(int i = oldLevel + 1; i <= level; i ++) {
                        newHead = new HeadIndex<>(oldBase, newHead, idxs[i], i);
                    }
                    if (casHead(header, newHead)) {
                        header = newHead;
                        idx = idxs[oldLevel];
                        break;
                    }
                }
            }
            Objects.requireNonNull(idx);
            int layer = level;

            while (oldLevel < layer) {
                header = header.down;
                layer--;
            }
            while (layer < oldLevel) {
                header = header.down;
                layer++;
            }
            out:
            for (;;) {//加入每层
                for (Index<K, V> h = header, r = header.right; ; ) {
                    if (r != null) {
                        K rk = r.node.key;
                        Node<K, V> n = r.node;
                        if (n.value == null) {
                            if (!h.unlink(r)) {
                                break;
                            }
                            r = h.right;
                            continue;
                        }
                        if (cm(key, rk) > 0) {
                            h = r;
                            r = r.right;
                            continue;
                        }
                    }
                    if (!(idx.casRight(idx.right, r) && h.casRight(r, idx))) {
                        break;
                    }
                    if (header.down == null) {
                        break out;
                    }
                    header = header.down;
                    idx = idx.down;
                }
            }
        }
        return null;
    }

    private boolean casHead(Index<K,V> header, HeadIndex<K,V> newHead) {
        return UNSAFE.compareAndSwapObject(this, headOffset, header, newHead);
    }

    /**
     * 通过索引落到最下层的基点
     * @param key
     */
    private Node<K, V> findPredecessor(Object key) {
        Objects.requireNonNull(key);
        for (;;) {
            for (Index<K, V> h = head, r = h.right, d; ; ) {
                if (r != null) {
                    K rk = r.node.key;
                    Node<K, V> n = r.node;
                    if (n.value == null) {//right节点被删
                        if (!h.unlink(r))//right节点替换
                            break;
                        r = h.right;//重读right节点
                        continue;
                    }
                    if (cm(key, rk) > 0) {
                        h = r;
                        r = r.right;
                        continue;
                    }
                }
                if ((d = h.down) == null)
                    return h.node;
                h = d;
                r = d.right;
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    int cm(Object key, Object rk) {
        return ((Comparable) key).compareTo(rk);
    }

    public void print() {
        Index header = head;
        while (header.down != null) {
            header = header.down;
        }
        Node node = header.node;
        if (node.next != null && node.next.value != null) {
            node = node.next;
        }
        while (node != null && node.value != null) {
            System.out.println(node.key);
            node = node.next;
        }
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

        boolean casRight(Index<K, V> except, Index<K, V> update) {
            return UNSAFE.compareAndSwapObject(this, rightOffset, except, update);
        }

        final boolean unlink(Index<K, V> r) {
            return node.value != null && casRight(r, r.right);
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

    private static final sun.misc.Unsafe UNSAFE;
    private static final long headOffset;
    static {
        try {
            UNSAFE = reflectUnsafe();
            Class<?> k = MiniSkipList.class;
            assert UNSAFE != null;
            headOffset = UNSAFE.objectFieldOffset
                    (k.getDeclaredField("head"));
        } catch (Exception e) {
            throw new Error(e);
        }
    }

}
