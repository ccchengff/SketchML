package org.dma.sketchml.sketch.binary;

import org.dma.sketchml.sketch.base.BinaryEncoder;
import org.dma.sketchml.sketch.util.Maths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.BitSet;

public class DeltaAdaptiveEncoder implements BinaryEncoder {
    private static final Logger LOG = LoggerFactory.getLogger(DeltaAdaptiveEncoder.class);

    private int size;
    private int numIntervals;   // how many number of intervals it splits [0, 31]
                                // should be exponential to 2
    private boolean flagKind;   // whether the number of flag bits is dynamic to different interval
    private BitSet deltaBits;
    private BitSet flagBits;

    private void calOptimalIntervals(double[] prob) {
        double optBitsPerKey = 32.0;
        numIntervals = 1;
        flagKind = false;
        for (int m = 2; m <= 16; m *= 2) {
            double[] intervalProb = new double[m];
            int b = 32 / m;
            double sum = 0.0;
            for (int i = 0; i < m; i++) {
                for (int j = 0; j < b; j++)
                    intervalProb[i] += prob[i * b + j];
                sum += (i + 1) * intervalProb[i];
            }
            // all flags have the same number of bits
            double t1 = sum * b + Maths.log2nlz(m);
            if (t1 < optBitsPerKey) {
                optBitsPerKey = t1;
                numIntervals = m;
                flagKind = false;
            }
            // one bit for each interval
            double t2 = sum * (b + 1) + 1;
            if (t2 < optBitsPerKey) {
                optBitsPerKey = t2;
                numIntervals = m;
                flagKind = true;
            }
        }
    }

    @Override
    public void encode(int[] values) {
        size = values.length;
        // 1. get probabilities of each range [2^i, 2^(i+1))
        int[] delta = new int[size];
        int[] bitsNeeded = new int[size];
        double[] prob = new double[32];
        delta[0] = values[0];
        if (delta[0] == 0)
            bitsNeeded[0] = 1;
        else
            bitsNeeded[0] = Maths.log2nlz(delta[0]) + 1;
        prob[bitsNeeded[0]]++;
        for (int i = 1; i < size; i++) {
            delta[i] = values[i] - values[i - 1];
            bitsNeeded[i] = Maths.log2nlz(delta[i]) + 1;
            prob[bitsNeeded[i]]++;
        }
        for (int i = 0; i < prob.length; i++)
            prob[i] /= size;
        // 2. get the optimal number of intervals, and the kind of flag bits
        calOptimalIntervals(prob);
        // 3. encode deltas
        deltaBits = new BitSet();
        flagBits = new BitSet();
        int bitsPerInterval = 32 / numIntervals;
        int bitsShift = Maths.log2nlz(bitsPerInterval);
        int flagOffset = 0, deltaOffset = 0;
        if (!flagKind) {
            int numBitsPerFlag = Maths.log2nlz(numIntervals);
            for (int i = 0; i < size; i++) {
                // ceil(bitsNeeded / bitsPerInterval)
                int intervalNeeded = (bitsNeeded[i] + bitsPerInterval - 1) >> bitsShift;
                // set flag
                BinaryUtils.setBits(flagBits, flagOffset, intervalNeeded - 1, numBitsPerFlag);
                flagOffset += numBitsPerFlag;
                // set delta
                BinaryUtils.setBits(deltaBits, deltaOffset, delta[i], bitsPerInterval * intervalNeeded);
                deltaOffset += bitsPerInterval * intervalNeeded;
            }
        } else {
            int[] flagCandidates = new int[numIntervals + 1];
            for (int i = 1; i <= numIntervals; i++) {
                // 0b1110 = 0b10000 - 2
                flagCandidates[i] = (1 << (i + 1)) - 2;
            }
            for (int i = 0; i < size; i++) {
                // ceil(bitsNeeded / bitsPerInterval)
                int intervalNeeded = (bitsNeeded[i] + bitsPerInterval - 1) >> bitsShift;
                // set flag
                BinaryUtils.setBits(flagBits, flagOffset, flagCandidates[intervalNeeded], intervalNeeded + 1);
                flagOffset += intervalNeeded + 1;
                // set delta
                BinaryUtils.setBits(deltaBits, deltaOffset, delta[i], bitsPerInterval * intervalNeeded);
                deltaOffset += bitsPerInterval * intervalNeeded;
            }
        }
        //LOG.info(String.format("BitsPerKey[%f], flag[%f], delta[%f]", (flagOffset + deltaOffset) * 1. / size,
        //        flagOffset * 1. / size, deltaOffset * 1. / size));
    }

    @Override
    public int[] decode() {
        int[] res = new int[size];
        int bitsPerInterval = 32 / numIntervals;
        int flagOffset = 0, deltaOffset = 0, prev = 0;;
        if (!flagKind) {
            int numBitsPerFlag = Maths.log2nlz(numIntervals);
            for (int i = 0; i < size; i++) {
                // get flag
                int intervalNeeded = BinaryUtils.getBits(flagBits, flagOffset, numBitsPerFlag) + 1;
                flagOffset += numBitsPerFlag;
                // get delta
                int delta = BinaryUtils.getBits(deltaBits, deltaOffset, bitsPerInterval * intervalNeeded);
                deltaOffset += bitsPerInterval * intervalNeeded;
                // set value
                res[i] = prev + delta;
                prev = res[i];
            }
        } else {
            for (int i = 0; i < size; i++) {
                // get flag
                int intervalNeeded = 0;
                while (flagBits.get(flagOffset++)) intervalNeeded++;
                // get delta
                int delta = BinaryUtils.getBits(deltaBits, deltaOffset, bitsPerInterval * intervalNeeded);
                deltaOffset += bitsPerInterval * intervalNeeded;
                // set value
                res[i] = prev + delta;
                prev = res[i];
            }
        }
        return res;
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
        oos.writeInt(size);
        oos.writeInt(numIntervals);
        oos.writeBoolean(flagKind);
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
        numIntervals = ois.readInt();
        flagKind = ois.readBoolean();
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
