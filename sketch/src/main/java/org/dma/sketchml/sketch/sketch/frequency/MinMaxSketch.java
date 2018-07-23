package org.dma.sketchml.sketch.sketch.frequency;

import org.dma.sketchml.sketch.base.Int2IntHash;
import org.dma.sketchml.sketch.hash.HashFactory;

import java.io.Serializable;
import java.util.Arrays;

public class MinMaxSketch implements Serializable {
    protected int rowNum;
    protected int colNum;
    protected int bitsPerCell;
    protected int[][] tables;
    protected int zeroValue;
    protected Int2IntHash[] hashes;
    public static final int DEFAULT_MINMAXSKETCH_ROW_NUM = 2;

    public MinMaxSketch(int rowNum, int colNum, int bitsPerCell, int zeroValue) {
        this.rowNum = rowNum;
        this.colNum = colNum;
        this.bitsPerCell = bitsPerCell;
        this.tables = new int[rowNum][colNum];
        this.zeroValue = zeroValue;
        int maxValue = compare(Integer.MIN_VALUE, Integer.MAX_VALUE) <= 0
                ? Integer.MIN_VALUE : Integer.MAX_VALUE;
        for (int i = 0; i < rowNum; i++) {
            Arrays.fill(tables[i], maxValue);
        }
        this.hashes = HashFactory.getRandomInt2IntHashes(rowNum, colNum);
    }

    public MinMaxSketch(int colNum, int bitsPerCell, int zeroValue) {
        this(DEFAULT_MINMAXSKETCH_ROW_NUM, colNum, bitsPerCell, zeroValue);
    }

    /**
     * Min: insert the minimal (closest to `zeroValue`) value
     *
     * @param key
     * @param value
     */
    public void insert(int key, int value) {
        for (int i = 0; i < rowNum; i++) {
            int code = hashes[i].hash(key);
            if (compare(value, tables[i][code]) < 0) {
                tables[i][code] = value;
            }
        }
    }


    /**
     * Max: return the maximal (furthest to `zeroValue`) value
     *
     * @param key
     * @return
     */
    public int query(int key) {
        int res = zeroValue;
        for (int i = 0; i < rowNum; i++) {
            int code = hashes[i].hash(key);
            if (compare(tables[i][code], res) > 0) {
                res = tables[i][code];
            }
        }
        return res;
    }

    /**
     * Compare two numbers' distances w.r.t. `zeroValue`
     *
     * @param v1
     * @param v2
     * @return
     */
    private int compare(int v1, int v2) {
        int d1 = Math.abs(v1 - zeroValue);
        int d2 = Math.abs(v2 - zeroValue);
        return d1 - d2;
    }

    public int memoryBytes() {
        // TODO: include the memory of hash functions
        return 16 + (int) Math.ceil(bitsPerCell * rowNum * colNum / 8.0);
    }
}
