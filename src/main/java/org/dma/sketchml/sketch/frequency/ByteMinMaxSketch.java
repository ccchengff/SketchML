package org.dma.sketchml.sketch.frequency;

import org.dma.sketchml.base.MinMaxSketch;

import java.util.Arrays;

public class ByteMinMaxSketch extends MinMaxSketch {
    private byte[][] tables;
    private byte zeroValue;

    public ByteMinMaxSketch(int rowNum, int colNum, byte zeroValue) {
        super(rowNum, colNum);
        this.zeroValue = zeroValue;
        this.tables = new byte[rowNum][colNum];
        byte maxValue = compare(Byte.MIN_VALUE, Byte.MAX_VALUE) <= 0
                ? Byte.MIN_VALUE : Byte.MAX_VALUE;
        for (int i = 0; i < rowNum; i++) {
            Arrays.fill(tables[i], maxValue);
        }
    }

    public ByteMinMaxSketch(int colNum, byte zeroValue) {
        this(DEFAULT_MINMAXSKETCH_ROW_NUM, colNum, zeroValue);
    }

    public void insert(int key, byte value) {
        for (int i = 0; i < rowNum; i++) {
            int code = hashes[i].hash(key);
            if (compare(value, tables[i][code]) < 0) {
                tables[i][code] = value;
            }
        }
    }

    public byte qurey(int key) {
        byte res = zeroValue;
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
    private int compare(byte v1, byte v2) {
        int d1 = Math.abs(((int) v1) - ((int) zeroValue));
        int d2 = Math.abs(((int) v2) - ((int) zeroValue));
        return d1 - d2;
    }

    @Override
    public int memoryBytes() {
        return super.memoryBytes() + rowNum * colNum + 1;
    }
}
