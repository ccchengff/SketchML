package org.dma.sketchml.sketch.binary;

import org.dma.sketchml.sketch.base.BinaryEncoder;
import org.dma.sketchml.sketch.base.SketchMLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.BitSet;
import java.util.stream.IntStream;

public class DeltaBinaryEncoder implements BinaryEncoder, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(DeltaBinaryEncoder.class);

    private BitSet deltaBits;
    private int size;
    private BitSet flagBits;

    @Override
    public void encode(int[] values) {
        size = values.length;
        flagBits = new BitSet(size * 2);
        deltaBits = new BitSet(size * 12);
        int offset = 0, prev = 0;
        for (int i = 0; i < size; i++) {
            int delta = values[i] - prev;
            int bytesNeeded = needBytes(delta);
            setBits(flagBits, 2 * i, bytesNeeded - 1, 2);
            setBytes(deltaBits, offset, delta, bytesNeeded);
            prev = values[i];
            offset += bytesNeeded * 8;
        }
    }

    @Override
    public void encode(IntStream stream) {
    }

    @Override
    public int[] decode() {
        int[] res = new int[size];
        int offset = 0, prev = 0;
        for (int i = 0; i < size; i++) {
            int bytesNeeded = getBits(flagBits, i * 2, 2) + 1;
            int delta = getBytes(deltaBits, offset, bytesNeeded);
            res[i] = prev + delta;
            prev = res[i];
            offset += bytesNeeded * 8;
        }
        return res;
    }

    public IntStream decodeAsStream() {
        return null;
    }

    public static int needBytes(int x) {
        if (x < 0) {
            throw new SketchMLException("Input of DeltaBinaryEncoder should be sorted");
        } else if (x < 256) {
            return 1;
        } else if (x < 65536) {
            return 2;
        } else {
            return 4;
        }
    }

    public static void setBits(BitSet bitSet, int offset, int value, int numBits) {
        for (int i = 0; i < numBits; i++) {
            boolean bit = (((value) >> i) & 1) == 1;
            bitSet.set(offset + i, bit);
        }
    }

    public static void setBytes(BitSet bitSet, int offset, int value, int numBytes) {
        setBits(bitSet, offset, value, numBytes * 8);
    }

    public static int getBits(BitSet bitSet, int offset, int numBits) {
        int res = 0;
        for (int i = 0; i < numBits; i++) {
            if (bitSet.get(offset + i)) {
                res |= 1 << i;
            }
        }
        return res;
    }

    public static int getBytes(BitSet bitSet, int offset, int numBytes) {
        return getBits(bitSet, offset, numBytes * 8);
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
        LOG.info("Serializing DeltaBinaryEncoder with size: " + size);
        oos.writeInt(size);
        long[] flags = flagBits.toLongArray();
        oos.writeInt(flags.length);
        for (long l : flags) {
            oos.writeLong(l);
        }
        long[] delta = deltaBits.toLongArray();
        oos.writeInt(delta.length);
        for (long l : delta) {
            oos.writeLong(l);
        }
    }

    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        size = ois.readInt();
        LOG.info("Deserializing DeltaBinaryEncoder with size: " + size);
        int flagsLength = ois.readInt();
        long[] flags = new long[flagsLength];
        for (int i = 0; i < flagsLength; i++) {
            flags[i] = ois.readLong();
        }
        flagBits = BitSet.valueOf(flags);
        int deltaLength = ois.readInt();
        long[] delta = new long[deltaLength];
        for (int i = 0; i < deltaLength; i++) {
            delta[i] = ois.readLong();
        }
        deltaBits = BitSet.valueOf(delta);
    }

    @Override
    public int memoryBytes() {
        return 4 + (int) (Math.ceil(flagBits.length() / 8.0) + Math.ceil(deltaBits.length() / 8.0));
    }
}
