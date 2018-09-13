package org.dma.sketchml.sketch.binary;

import org.dma.sketchml.sketch.base.BinaryEncoder;
import org.dma.sketchml.sketch.base.SketchMLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.BitSet;

/**
 * This is the special case for DeltaAdaptiveEncoder
 * where numIntervals equals to 4 and number of flag bits is constant
 *
 * */
public class DeltaBinaryEncoder implements BinaryEncoder {
    private static final Logger LOG = LoggerFactory.getLogger(DeltaBinaryEncoder.class);

    private int size;
    private BitSet deltaBits;
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
            BinaryUtils.setBits(flagBits, 2 * i, bytesNeeded - 1, 2);
            BinaryUtils.setBytes(deltaBits, offset, delta, bytesNeeded);
            prev = values[i];
            offset += bytesNeeded * 8;
        }
    }

    @Override
    public int[] decode() {
        int[] res = new int[size];
        int offset = 0, prev = 0;
        for (int i = 0; i < size; i++) {
            int bytesNeeded = BinaryUtils.getBits(flagBits, i * 2, 2) + 1;
            int delta = BinaryUtils.getBytes(deltaBits, offset, bytesNeeded);
            res[i] = prev + delta;
            prev = res[i];
            offset += bytesNeeded * 8;
        }
        return res;
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

    private void writeObject(ObjectOutputStream oos) throws IOException {
        oos.writeInt(size);
        if (flagBits == null) {
            oos.writeInt(0);
        } else {
            long[] flags = flagBits.toLongArray();
            oos.writeInt(flags.length);
            for (long l : flags) {
                oos.writeLong(l);
            }
        }
        if (deltaBits == null) {
            oos.writeInt(0);
        } else {
            long[] delta = deltaBits.toLongArray();
            oos.writeInt(delta.length);
            for (long l : delta) {
                oos.writeLong(l);
            }
        }
    }

    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        size = ois.readInt();
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
}
