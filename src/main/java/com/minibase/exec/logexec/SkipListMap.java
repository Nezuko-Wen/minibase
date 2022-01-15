package com.minibase.exec.logexec;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.*;
import java.util.function.*;
import java.util.stream.Stream;

/**
 * @author zouzhiwen
 * @date 2022/1/10 16:27
 */
public class SkipListMap<K, V> extends AbstractMap<K, V> {
    private Index<K, V> head;

    private static final sun.misc.Unsafe UNSAFE;
    private static final long valueOffset;
    private static final long nextOffset;

    static {
        try {
            UNSAFE = reflectUnsafe();
            assert UNSAFE != null;
            Class<?> k = MiniSkipList.Node.class;
            valueOffset = UNSAFE.objectFieldOffset
                    (k.getDeclaredField("value"));
            nextOffset = UNSAFE.objectFieldOffset
                    (k.getDeclaredField("next"));
        } catch (Exception e) {
            throw new Error(e);
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

    public Set<Map.Entry<K, V>> entrySet() {
        return null;
    }

    static class Index<K, V> {
        final MiniSkipList.Node<K, V> node;
        volatile MiniSkipList.Index<K, V> down;
        volatile MiniSkipList.Index<K, V> right;

        Index(MiniSkipList.Node<K, V> node, MiniSkipList.Index<K, V> down, MiniSkipList.Index<K, V> right) {
            this.node = node;
            this.down = down;
            this.right = right;
        }
    }

    static class Node<K, V> {
        final K key;
        volatile Object value;

        Node(K key, Object value) {
            this.key = key;
            this.value = value;
        }
    }

    static final class KeySet<K> extends AbstractSet<K> implements NavigableSet<K> {

        public K lower(K k) {
            return null;
        }

        public K floor(K k) {
            return null;
        }

        public K ceiling(K k) {
            return null;
        }

        public K higher(K k) {
            return null;
        }

        public K pollFirst() {
            return null;
        }

        public K pollLast() {
            return null;
        }

        public Iterator<K> iterator() {
            return null;
        }

        public NavigableSet<K> descendingSet() {
            return null;
        }

        public Iterator<K> descendingIterator() {
            return null;
        }

        public NavigableSet<K> subSet(K fromElement, boolean fromInclusive, K toElement, boolean toInclusive) {
            return null;
        }

        public NavigableSet<K> headSet(K toElement, boolean inclusive) {
            return null;
        }

        public NavigableSet<K> tailSet(K fromElement, boolean inclusive) {
            return null;
        }

        public Comparator<? super K> comparator() {
            return null;
        }

        public SortedSet<K> subSet(K fromElement, K toElement) {
            return null;
        }

        public SortedSet<K> headSet(K toElement) {
            return null;
        }

        public SortedSet<K> tailSet(K fromElement) {
            return null;
        }

        public K first() {
            return null;
        }

        public K last() {
            return null;
        }

        public void forEach(Consumer<? super K> action) {
        }

        public boolean removeIf(Predicate<? super K> filter) {
            return false;
        }

        public Spliterator<K> spliterator() {
            return null;
        }

        public Stream<K> stream() {
            return null;
        }

        public Stream<K> parallelStream() {
            return null;
        }

        public int size() {
            return 0;
        }
    }

    static final class Values<E> implements Collection<E> {

        public int size() {
            return 0;
        }

        public boolean isEmpty() {
            return false;
        }

        public boolean contains(Object o) {
            return false;
        }

        public Iterator<E> iterator() {
            return null;
        }

        public Object[] toArray() {
            return new Object[0];
        }

        public <T> T[] toArray(T[] a) {
            return null;
        }

        public boolean add(E e) {
            return false;
        }

        public boolean remove(Object o) {
            return false;
        }

        public boolean containsAll(Collection<?> c) {
            return false;
        }

        public boolean addAll(Collection<? extends E> c) {
            return false;
        }

        public boolean removeAll(Collection<?> c) {
            return false;
        }

        public boolean retainAll(Collection<?> c) {
            return false;
        }

        public void clear() {
        }
    }

    static final class EntrySet<K1, V1> extends AbstractSet<Map.Entry<K1, V1>> {

        public Iterator<Map.Entry<K1, V1>> iterator() {
            return null;
        }

        public void forEach(Consumer<? super Map.Entry<K1, V1>> action) {
        }

        public boolean removeIf(Predicate<? super Map.Entry<K1, V1>> filter) {
            return false;
        }

        public Spliterator<Map.Entry<K1, V1>> spliterator() {
            return null;
        }

        public Stream<Map.Entry<K1, V1>> stream() {
            return null;
        }

        public Stream<Map.Entry<K1, V1>> parallelStream() {
            return null;
        }

        public int size() {
            return 0;
        }
    }
}
