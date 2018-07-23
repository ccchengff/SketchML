package org.dma.sketchml.sketch.hash;

import org.dma.sketchml.sketch.base.Int2IntHash;

public class BKDRHash extends Int2IntHash {
    private int seed;

    public BKDRHash(int size, int seed) {
        super(size);
        this.seed = seed;
    }

    public int hash(int key) {
        int code = 0;
        while (key != 0) {
            code = seed * code + (key % 10);
            key /= 10;
        }
        code %= size;
        return code >= 0 ? code : code + size;
    }

    @Override
    public Int2IntHash clone() {
        return new BKDRHash(size, seed);
    }

    public int getSeed() {
        return seed;
    }
}
