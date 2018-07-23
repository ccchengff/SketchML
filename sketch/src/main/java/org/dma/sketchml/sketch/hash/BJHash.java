package org.dma.sketchml.sketch.hash;

import org.dma.sketchml.sketch.base.Int2IntHash;

public class BJHash extends Int2IntHash {
    public BJHash(int size) {
        super(size);
    }

    public int hash(int key) {
        int code = key;
        code = (code + 0x7ed55d16) + (code << 12);
        code = (code ^ 0xc761c23c) ^ (code >> 19);
        code = (code + 0x165667b1) + (code << 5);
        code = (code + 0xd3a2646c) ^ (code << 9);
        code = (code + 0xfd7046c5) + (code << 3);
        code = (code ^ 0xb55a4f09) ^ (code >> 16);
        code %= size;
        return code >= 0 ? code : code + size;
    }

    @Override
    public Int2IntHash clone() {
        return new BJHash(size);
    }
}
