package org.dma.sketchml.sketch.binary;

import java.util.BitSet;

public class BinaryUtils {
    public static void setBits(BitSet bitSet, int offset, int value, int numBits) {
        for (int i = numBits - 1; i >= 0; i--) {
            int t = value - (1 << i);
            if (t >= 0) {
                bitSet.set(offset + numBits - 1 - i);
                value = t;
            }
        }
    }

    public static void setBytes(BitSet bitSet, int offset, int value, int numBytes) {
        setBits(bitSet, offset, value, numBytes * 8);
    }

    public static int getBits(BitSet bitSet, int offset, int numBits) {
        int res = 0;
        for (int i = 0; i < numBits; i++) {
            res <<= 1;
            res |= bitSet.get(offset + i) ? 1 : 0;
            if (bitSet.get(offset + i))
                res |= 1;
        }
        return res;
    }

    public static int getBytes(BitSet bitSet, int offset, int numBytes) {
        return getBits(bitSet, offset, numBytes * 8);
    }

    public static String bits2String(int value, int numBits) {
        StringBuilder sb = new StringBuilder();
        for (int i = numBits - 1; i >= 0; i--) {
            int t = value - (1 << i);
            if (t < 0) {
                sb.append("0");
            } else {
                sb.append("1");
                value = t;
            }
        }
        return sb.toString();
    }

    public static String bits2String(BitSet bitset, int from, int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = from; i < from + length; i++)
            sb.append(bitset.get(i) ? 1 : 0);
        return sb.toString();
    }
}
