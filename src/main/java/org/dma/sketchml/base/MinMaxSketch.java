package org.dma.sketchml.base;

import org.dma.sketchml.hash.HashFactory;

import java.io.Serializable;

public abstract class MinMaxSketch implements Serializable {
    protected int rowNum;
    protected int colNum;
    protected Int2IntHash[] hashes;
    public static final int DEFAULT_MINMAXSKETCH_ROW_NUM = 2;

    public MinMaxSketch(int rowNum, int colNum) {
        this.rowNum = rowNum;
        this.colNum = colNum;
        this.hashes = HashFactory.getRandomInt2IntHashes(rowNum, colNum);
    }

    public int memoryBytes() {
        return 8; // TODO: include the memory of hash functions
    }
}
