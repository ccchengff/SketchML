package org.dma.sketchml.hash;

import org.dma.sketchml.base.Int2IntHash;

public class TWHash extends Int2IntHash {
    public TWHash(int size) {
        super(size);
    }

    public int hash(int key) {
        int code = key;
        code = ~code + (code << 15);
        code = code ^ (code >> 12);
        code = code + (code << 2);
        code = code ^ (code >> 4);
        code = code * 2057;
        code = code ^ (code >> 16);
        code %= size;
        return code >= 0 ? code : code + size;
    }

    @Override
    public Int2IntHash clone() {
        return new TWHash(size);
    }
}
