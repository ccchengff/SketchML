package org.dma.sketchml.sketch.sketch.frequency;

import org.dma.sketchml.sketch.base.BinaryEncoder;
import org.dma.sketchml.sketch.base.Int2IntHash;
import org.dma.sketchml.sketch.binary.HuffmanEncoder;
import org.dma.sketchml.sketch.hash.HashFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;

public class MinMaxSketch implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(MinMaxSketch.class);

    protected int rowNum;
    protected int colNum;
    protected int[] table;
    protected int zeroValue;
    protected Int2IntHash[] hashes;

    public static final int DEFAULT_MINMAXSKETCH_ROW_NUM = 2;

    public MinMaxSketch(int rowNum, int colNum, int zeroValue) {
        this.rowNum = rowNum;
        this.colNum = colNum;
        this.table = new int[rowNum * colNum];
        this.zeroValue = zeroValue;
        int maxValue = compare(Integer.MIN_VALUE, Integer.MAX_VALUE) <= 0
                ? Integer.MIN_VALUE : Integer.MAX_VALUE;
        Arrays.fill(table, maxValue);
        this.hashes = HashFactory.getRandomInt2IntHashes(rowNum, colNum);
    }

    public MinMaxSketch(int colNum, int zeroValue) {
        this(DEFAULT_MINMAXSKETCH_ROW_NUM, colNum, zeroValue);
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
            int index = i * colNum + code;
            if (compare(value, table[index]) < 0)
                table[index] = value;
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
            int index = i * colNum + code;
            if (compare(table[index], res) > 0)
                res = table[index];
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

    private void writeObject(ObjectOutputStream oos) throws IOException {
        oos.writeInt(rowNum);
        oos.writeInt(colNum);
        oos.writeInt(zeroValue);
        for (Int2IntHash hash : hashes)
            HashFactory.serialize(oos, hash);
        BinaryEncoder huffman = new HuffmanEncoder();
        huffman.encode(table);
        oos.writeObject(huffman);
    }

    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        rowNum = ois.readInt();
        colNum = ois.readInt();
        zeroValue = ois.readInt();
        hashes = new Int2IntHash[rowNum];
        for (int i = 0; i < rowNum; i++)
            hashes[i] = HashFactory.deserialize(ois);
        BinaryEncoder encoder = (BinaryEncoder) ois.readObject();
        table = encoder.decode();
    }

    public int getRowNum() {
        return rowNum;
    }

    public int getColNum() {
        return colNum;
    }

    public int getZeroValue() {
        return zeroValue;
    }
}
