package org.dma.sketchml.sketch.base;

import java.io.Serializable;

public abstract class Int2IntHash implements Serializable {
    protected int size;

    public Int2IntHash(int size) {
        this.size = size;
    }

    public abstract int hash(int key);

    public abstract Int2IntHash clone();

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}
