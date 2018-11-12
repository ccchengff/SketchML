package org.dma.sketchml.sketch.hash;

import org.dma.sketchml.sketch.base.Int2IntHash;

public class TWHash extends Int2IntHash {
    public TWHash(int size) {
        super(size);
    }

    protected int doHash(int key) {
        int code = key;
        code = ~code + (code << 15);
        code = code ^ (code >> 12);
        code = code + (code << 2);
        code = code ^ (code >> 4);
        code = code * 2057;
        code = code ^ (code >> 16);
        return code;
    }

    @Override
    public Int2IntHash clone() {
        TWHash copy = new TWHash(size);
        copy.buffer = this.buffer;
        return copy;
    }

    @Override
    public boolean equalsIgnoreSize(Int2IntHash other) {
        return other instanceof TWHash;
    }
}
