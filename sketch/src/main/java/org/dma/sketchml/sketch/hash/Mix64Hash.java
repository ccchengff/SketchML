package org.dma.sketchml.sketch.hash;

import org.dma.sketchml.sketch.base.Int2IntHash;

public class Mix64Hash extends Int2IntHash {
    public Mix64Hash(int size) {
        super(size);
    }

    public int hash(int key) {
        int code = key;
        code = (~code) + (code << 21); // code = (code << 21) - code - 1;
        code = code ^ (code >> 24);
        code = (code + (code << 3)) + (code << 8); // code * 265
        code = code ^ (code >> 14);
        code = (code + (code << 2)) + (code << 4); // code * 21
        code = code ^ (code >> 28);
        code = code + (code << 31);
        code %= size;
        return code >= 0 ? code : code + size;
    }

    @Override
    public Int2IntHash clone() {
        return new Mix64Hash(size);
    }
}
