package org.dma.sketchml.compressor;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.dma.sketchml.base.BinaryEncoder;
import org.dma.sketchml.base.Quantizer;
import org.dma.sketchml.base.SketchMLException;
import org.dma.sketchml.binary.DeltaBinaryEncoder;
import org.dma.sketchml.quantization.QuantileQuantizer;
import org.dma.sketchml.quantization.UniformQuantizer;
import org.dma.sketchml.sketch.frequency.ByteMinMaxSketch;
import org.dma.sketchml.sketch.frequency.IntMinMaxSketch;
import org.dma.sketchml.sketch.frequency.ShortMinMaxSketch;

public abstract class CompressUtil {
    public static Quantizer newQuantizer(Quantizer.QuantizationType type, int binNum) {
        switch (type) {
            case QUANTILE:
                return new QuantileQuantizer(binNum);
            case UNIFORM:
                return new UniformQuantizer(binNum);
            default:
                throw new SketchMLException(
                        "Unrecognizable quantization type: " + type);
        }
    }

    public static int[] calGroupEdges(int zeroIdx, int binNum, int groupNum) {
        if (groupNum == 2) {
            return new int[]{zeroIdx, binNum};
        } else {
            int[] groupEdges = new int[groupNum];
            int binsPerGroup = binNum / groupNum;
            if (zeroIdx < binsPerGroup) {
                groupEdges[0] = zeroIdx;
            } else if ((zeroIdx % binsPerGroup) < (binsPerGroup / 2)) {
                groupEdges[0] = binsPerGroup + zeroIdx % binsPerGroup;
            } else {
                groupEdges[0] = zeroIdx % binsPerGroup;
            }
            for (int i = 1; i < groupNum - 1; i++) {
                groupEdges[i] = groupEdges[i - 1] + binsPerGroup;
            }
            groupEdges[groupNum - 1] = binNum;
            return groupEdges;
        }
    }

    public static Pair<IntArrayList[], ByteArrayList[]> partition(int[] keys, byte[] bins, int[] groupEdges) {
        int groupNum = groupEdges.length;
        IntArrayList[] keyLists = new IntArrayList[groupNum];
        ByteArrayList[] binLists = new ByteArrayList[groupNum];
        for (int i = 0; i < groupNum; i++) {
            int groupSpan = i > 0 ? (groupEdges[i] - groupEdges[i - 1]) : groupEdges[0];
            int estimatedGroupSize = (int) (1.0 * keys.length / groupNum * groupSpan);
            keyLists[i] = new IntArrayList(estimatedGroupSize);
            binLists[i] = new ByteArrayList(estimatedGroupSize);
        }
        for (int i = 0; i < keys.length; i++) {
            int binIdx = ((int) bins[i]) - Byte.MIN_VALUE;
            int groupIdx = 0;
            while (groupEdges[groupIdx] <= binIdx) groupIdx++;
            keyLists[groupIdx].add(keys[i]);
            binLists[groupIdx].add(bins[i]);
        }
        return new ImmutablePair<>(keyLists, binLists);
    }

    public static Pair<IntArrayList[], ShortArrayList[]> partition(int[] keys, short[] bins, int[] groupEdges) {
        int groupNum = groupEdges.length;
        IntArrayList[] keyLists = new IntArrayList[groupNum];
        ShortArrayList[] binLists = new ShortArrayList[groupNum];
        for (int i = 0; i < groupNum; i++) {
            int groupSpan = i > 0 ? (groupEdges[i] - groupEdges[i - 1]) : groupEdges[0];
            int estimatedGroupSize = (int) (1.0 * keys.length / groupNum * groupSpan);
            keyLists[i] = new IntArrayList(estimatedGroupSize);
            binLists[i] = new ShortArrayList(estimatedGroupSize);
        }
        for (int i = 0; i < keys.length; i++) {
            int binIdx = ((int) bins[i]) - Short.MIN_VALUE;
            int groupIdx = 0;
            while (groupEdges[groupIdx] <= binIdx) groupIdx++;
            keyLists[groupIdx].add(keys[i]);
            binLists[groupIdx].add(bins[i]);
        }
        return new ImmutablePair<>(keyLists, binLists);
    }

    public static Pair<IntArrayList[], IntArrayList[]> partition(int[] keys, int[] bins, int[] groupEdges) {
        int groupNum = groupEdges.length;
        IntArrayList[] keyLists = new IntArrayList[groupNum];
        IntArrayList[] binLists = new IntArrayList[groupNum];
        for (int i = 0; i < groupNum; i++) {
            int groupSpan = i > 0 ? (groupEdges[i] - groupEdges[i - 1]) : groupEdges[0];
            int estimatedGroupSize = (int) Math.ceil(1.0 * keys.length / groupNum * groupSpan);
            keyLists[i] = new IntArrayList(estimatedGroupSize);
            binLists[i] = new IntArrayList(estimatedGroupSize);
        }
        for (int i = 0; i < keys.length; i++) {
            int binIdx = bins[i] - Integer.MIN_VALUE;
            int groupIdx = 0;
            while (groupEdges[groupIdx] <= binIdx) groupIdx++;
            keyLists[groupIdx].add(keys[i]);
            binLists[groupIdx].add(bins[i]);
        }
        return new ImmutablePair<>(keyLists, binLists);
    }

    public static ByteMinMaxSketch encodeByteBins(IntArrayList keyList, ByteArrayList binList,
                                            byte zeroValue, int rowNum, double colRatio) {
        int groupSize = keyList.size();
        int colNum = (int) Math.ceil(groupSize * colRatio);
        ByteMinMaxSketch sketch = new ByteMinMaxSketch(rowNum, colNum, zeroValue);
        for (int i = 0; i < groupSize; i++) {
            sketch.insert(keyList.getInt(i), binList.getByte(i));
        }
        return sketch;
    }

    public static ShortMinMaxSketch encodeShortBins(IntArrayList keyList, ShortArrayList binList,
                                              short zeroValue, int rowNum, double colRatio) {
        int groupSize = keyList.size();
        int colNum = (int) Math.ceil(groupSize * colRatio);
        ShortMinMaxSketch sketch = new ShortMinMaxSketch(rowNum, colNum, zeroValue);
        for (int i = 0; i < groupSize; i++) {
            sketch.insert(keyList.getInt(i), binList.getShort(i));
        }
        return sketch;
    }

    public static IntMinMaxSketch encodeIntBins(IntArrayList keyList, IntArrayList binList,
                                          int zeroValue, int rowNum, double colRatio) {
        int groupSize = keyList.size();
        int colNum = (int) Math.ceil(groupSize * colRatio);
        IntMinMaxSketch sketch = new IntMinMaxSketch(rowNum, colNum, zeroValue);
        for (int i = 0; i < groupSize; i++) {
            sketch.insert(keyList.getInt(i), binList.getInt(i));
        }
        return sketch;
    }


    public static BinaryEncoder encodeKeys(IntArrayList keyList) {
        BinaryEncoder encoder = new DeltaBinaryEncoder();
        encoder.encode(keyList.toIntArray());
        return encoder;
    }
}
