package com.hazelcast.hotrestart.visualgc;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * Holds a snapshot of a HotRestart store state as read from a file written
 * by {@code com.hazelcast.spi.hotrestart.impl.gc.Snapshotter}.
 */
class Snapshot {
    private static final int DEFAULT_INITIAL_CAPACITY = 1024;
    private static final int ARRAY_ITEMS_PER_CHUNK = 2;
    private static final long SIZE_MASK = 0xffff00L;
    private static final long CHAR_MASK = 0xffffL;
    private static final long BYTE_MASK = 0xffL;
    private static final int SOURCE_CHUNK_FLAG_MASK = 1;
    private static final int SURVIVOR_FLAG_MASK = 1 << 1;
    private static final int TOMBSTONE_FLAG_MASK = 1 << 2;

    private final long[] store;

    private int chunkCount;

    Snapshot() {
        store = newStore(DEFAULT_INITIAL_CAPACITY);
    }

    private Snapshot(int chunkCount) {
        this.chunkCount = chunkCount;
        store = newStore(chunkCount);
    }

    Snapshot withEnoughCapacity(int chunkCount) {
        this.chunkCount = chunkCount;
        return chunkCount <= capacity() ? this : new Snapshot(chunkCount);
    }

    void refreshFrom(DataInputStream in) throws IOException {
        for (int i = 0; i < chunkCount; i++) {
            setSeqAt(i, in.readLong());
            setSizeGarbageFlagsAt(i, in.readChar(), in.readChar(), in.readByte());
        }
        sortStoreBySeq();
    }

    int chunkCount() {
        return chunkCount;
    }

    long seqAt(long i) {
        return store[baseIndex(i)];
    }

    long sizeGarbageFlagsAt(long i) {
        return store[baseIndex(i) + 1];
    }

    int sizeAt(long i) {
        long r = sizeGarbageFlagsAt(i);
        r >>>= Character.SIZE;
        r &= SIZE_MASK;
        return (int) r;
    }

    int garbageAt(long i) {
        long r = sizeGarbageFlagsAt(i);
        r &= SIZE_MASK;
        return (int) r;
    }

    boolean isSelectedForGcAt(long i) {
        return (sizeGarbageFlagsAt(i) & SOURCE_CHUNK_FLAG_MASK) != 0;
    }

    boolean isGcSurvivorAt(long i) {
        return (sizeGarbageFlagsAt(i) & SURVIVOR_FLAG_MASK) != 0;
    }

    boolean isTombstoneChunkAt(long i) {
        return (sizeGarbageFlagsAt(i) & TOMBSTONE_FLAG_MASK) != 0;
    }

    void setSeqAt(long i, long seq) {
        store[baseIndex(i)] = seq;
    }

    void setSizeGarbageFlagsAt(long i, long sizeGarbageFlags) {
        store[baseIndex(i) + 1] = sizeGarbageFlags;
    }

    private void setSizeGarbageFlagsAt(long i, char encodedSize, char encodedGarbage, byte flags) {
        long encoded = encodedSize & CHAR_MASK;
        encoded <<= Character.SIZE;
        encoded |= encodedGarbage & CHAR_MASK;
        encoded <<= Byte.SIZE;
        encoded |= flags & BYTE_MASK;
        store[baseIndex(i) + 1] = encoded;
    }

    private int capacity() {
        return store.length / ARRAY_ITEMS_PER_CHUNK;
    }

    private static long[] newStore(int chunkCount) {
        return new long[arraySizeForChunkCount(nextPowerOfTwo(chunkCount))];
    }

    private static int arraySizeForChunkCount(int chunkCount) {
        return ARRAY_ITEMS_PER_CHUNK * chunkCount;
    }

    private static int baseIndex(long i) {
        return ARRAY_ITEMS_PER_CHUNK * (int) i;
    }

    private static int nextPowerOfTwo(int value) {
        return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
    }

    private void sortStoreBySeq() {
        quickSort(0, chunkCount() - 1);
    }

    private void quickSort(int lo, int hi) {
        if (lo >= hi) {
            return;
        }
        int p = partition(lo, hi);
        quickSort(lo, p);
        quickSort(p + 1, hi);
    }

    private int partition(int lo, int hi) {
        final long pivot = seqAt((lo + hi) >>> 1);
        int i = lo - 1;
        int j = hi + 1;
        while (true) {
            do {
                i++;
            } while (seqAt(i) < pivot);
            do {
                j--;
            } while (seqAt(j) > pivot);
            if (i >= j) {
                return j;
            }
            swap(i, j);
        }
    }

    private void swap(long index1, long index2) {
        long tmpSeq = seqAt(index1);
        long tmpSizeGarbageFlags = sizeGarbageFlagsAt(index1);
        setSeqAt(index1, seqAt(index2));
        setSizeGarbageFlagsAt(index1, sizeGarbageFlagsAt(index2));
        setSeqAt(index2, tmpSeq);
        setSizeGarbageFlagsAt(index2, tmpSizeGarbageFlags);
    }
}
