package org.dma.sketchml.sketch.frequency;

import org.dma.sketchml.base.MinMaxSketch;

import java.util.Arrays;

public class ShortMinMaxSketch extends MinMaxSketch {
    private short[][] tables;
    private short zeroValue;

    public ShortMinMaxSketch(int rowNum, int colNum, short zeroValue) {
        super(rowNum, colNum);
        this.zeroValue = zeroValue;
        this.tables = new short[rowNum][colNum];
        short maxValue = compare(Short.MIN_VALUE, Short.MAX_VALUE) <= 0
                ? Short.MIN_VALUE : Short.MAX_VALUE;
        for (int i = 0; i < rowNum; i++) {
            Arrays.fill(tables[i], maxValue);
        }
    }

    public ShortMinMaxSketch(int colNum, short zeroValue) {
        this(DEFAULT_MINMAXSKETCH_ROW_NUM, colNum, zeroValue);
    }

    public void insert(int key, short value) {
        for (int i = 0; i < rowNum; i++) {
            int code = hashes[i].hash(key);
            if (compare(value, tables[i][code]) < 0) {
                tables[i][code] = value;
            }
        }
    }

    public short qurey(int key) {
        short res = zeroValue;
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
    private int compare(short v1, short v2) {
        int d1 = Math.abs(((int) v1) - ((int) zeroValue));
        int d2 = Math.abs(((int) v2) - ((int) zeroValue));
        return d1 - d2;
    }

    @Override
    public int memoryBytes() {
        return super.memoryBytes() + rowNum * colNum * 2 + 2;
    }
}
