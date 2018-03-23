package org.dma.sketchml.sketch.frequency;

import org.dma.sketchml.base.MinMaxSketch;

import java.util.Arrays;

public class IntMinMaxSketch extends MinMaxSketch {
    private int[][] tables;
    private int zeroValue;

    public IntMinMaxSketch(int rowNum, int colNum, int zeroValue) {
        super(rowNum, colNum);
        this.zeroValue = zeroValue;
        this.tables = new int[rowNum][colNum];
        int maxValue = compare(Integer.MIN_VALUE, Integer.MAX_VALUE) <= 0
                ? Integer.MIN_VALUE : Integer.MAX_VALUE;
        for (int i = 0; i < rowNum; i++) {
            Arrays.fill(tables[i], maxValue);
        }
    }

    public IntMinMaxSketch(int colNum, int zeroValue) {
        this(DEFAULT_MINMAXSKETCH_ROW_NUM, colNum, zeroValue);
    }

    public void insert(int key, int value) {
        for (int i = 0; i < rowNum; i++) {
            int code = hashes[i].hash(key);
            if (compare(value, tables[i][code]) < 0) {
                tables[i][code] = value;
            }
        }
    }

    public int qurey(int key) {
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

    @Override
    public int memoryBytes() {
        return super.memoryBytes() + rowNum * colNum * 4 + 4;
    }
}
