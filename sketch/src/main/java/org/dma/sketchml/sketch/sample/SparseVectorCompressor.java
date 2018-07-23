package org.dma.sketchml.sketch.sample;


import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.dma.sketchml.sketch.util.Maths;
import org.dma.sketchml.sketch.util.Sort;
import org.dma.sketchml.sketch.base.BinaryEncoder;
import org.dma.sketchml.sketch.base.Quantizer;
import org.dma.sketchml.sketch.base.SketchMLException;
import org.dma.sketchml.sketch.base.VectorCompressor;
import org.dma.sketchml.sketch.binary.DeltaBinaryEncoder;
import org.dma.sketchml.sketch.common.Constants;
import org.dma.sketchml.sketch.sketch.frequency.MinMaxSketch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class SparseVectorCompressor implements VectorCompressor, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(SparseVectorCompressor.class);

    private int size;

    private Quantizer.QuantizationType quantType;
    private int quantBinNum;
    private double[] quantValues;

    private int mmSketchGroupNum;
    private int mmSketchRowNum;
    private double mmSketchColRatio;
    private MinMaxSketch[] minMaxSketches;

    private BinaryEncoder[] encoders;

    public SparseVectorCompressor(
            Quantizer.QuantizationType quantType, int quantBinNum,
            int mmSketchGroupNum, int mmSketchRowNum, double mmSketchColRatio) {
        this.quantType = quantType;
        this.quantBinNum = quantBinNum;
        this.mmSketchGroupNum = mmSketchGroupNum;
        this.mmSketchRowNum = mmSketchRowNum;
        this.mmSketchColRatio = mmSketchColRatio;
    }

    @Override
    public void compressDense(double[] values) {
        LOG.warn("Compressing a dense vector with SparseVectorCompressor");
        int[] keys = new int[values.length];
        Arrays.setAll(keys, i -> i);
        compressSparse(keys, values);
    }

    @Override
    public void compressSparse(int[] keys, double[] values) {
        long startTime = System.currentTimeMillis();
        if (keys.length != values.length) {
            throw new SketchMLException(String.format(
                    "Lengths of key array and value array do not match: %d, %d",
                    keys.length, values.length));
        }
        size = keys.length;
        // 1. quantize into bin indexes
        Quantizer quantizer = CompressUtil.newQuantizer(quantType, quantBinNum);
        quantizer.quantize(values);
        quantValues = quantizer.getValues();
        // 2. divide bins into several groups
        int[] groupEdges = CompressUtil.calGroupEdges(quantizer.getZeroIdx(),
                quantBinNum, mmSketchGroupNum);
        minMaxSketches = new MinMaxSketch[mmSketchGroupNum];
        encoders = new BinaryEncoder[mmSketchGroupNum];
        // 3 & 4. encode bins and keys
        Pair<IntArrayList[], IntArrayList[]> partKBLists = CompressUtil.partition(
                keys, quantizer.getBins(), groupEdges);
        int zeroValue = quantizer.getZeroIdx();
        for (int i = 0; i < mmSketchGroupNum; i++) {
            IntArrayList keyList = partKBLists.getLeft()[i];
            IntArrayList binList = partKBLists.getRight()[i];
            // 3. encode bins
            int groupSize = keyList.size();
            int mmSketchColNum = (int) Math.ceil(groupSize * mmSketchColRatio);
            int partNumBin = groupEdges[i] - i == 0 ? 0 : groupEdges[i - 1];
            int bitsPerCell = Maths.log2nlz(partNumBin);
            minMaxSketches[i] = new MinMaxSketch(mmSketchRowNum, mmSketchColNum, bitsPerCell, zeroValue);
            for (int j = 0; j < groupSize; j++) {
                minMaxSketches[i].insert(keyList.getInt(j), binList.getInt(j));
            }
            // 4. encode keys
            encoders[i] = new DeltaBinaryEncoder();
            encoders[i].encode(keyList.toIntArray(null));
        }
        LOG.debug(String.format("Sparse vector compression cost %d ms, %d key-value " +
                "pairs in total", System.currentTimeMillis() - startTime, size));
    }

    @Override
    public void parallelCompressDense(double[] values) throws InterruptedException, ExecutionException {
        LOG.warn("Compressing a dense vector with SparseVectorCompressor");
        int[] keys = new int[values.length];
        Arrays.setAll(keys, i -> i);
        parallelCompressSparse(keys, values);
    }

    @Override
    public void parallelCompressSparse(int[] keys, double[] values) throws InterruptedException, ExecutionException {
        long startTime = System.currentTimeMillis();
        if (keys.length != values.length) {
            throw new SketchMLException(String.format(
                    "Lengths of key array and value array do not match: %d, %d",
                    keys.length, values.length));
        }
        size = keys.length;
        // 1. quantize into bin indexes
        Quantizer quantizer = CompressUtil.newQuantizer(quantType, quantBinNum);
        quantizer.parallelQuantize(values);
        quantValues = quantizer.getValues();
        // 2. divide bins into several groups
        int[] groupEdges = CompressUtil.calGroupEdges(quantizer.getZeroIdx(),
                quantBinNum, mmSketchGroupNum);
        minMaxSketches = new MinMaxSketch[mmSketchGroupNum];
        encoders = new BinaryEncoder[mmSketchGroupNum];
        // 3 & 4. encode bins and keys
        ExecutorService threadPool = Constants.Parallel.getThreadPool();
        Future<Void>[] futures = new Future[mmSketchGroupNum];
        Pair<IntArrayList[], IntArrayList[]> partKBLists = CompressUtil.partition(
                keys, quantizer.getBins(), groupEdges);
        int zeroValue = quantizer.getZeroIdx();
        for (int i = 0; i < mmSketchGroupNum; i++) {
            int groupIdx = i;
            futures[i] = threadPool.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    IntArrayList keyList = partKBLists.getLeft()[groupIdx];
                    IntArrayList binList = partKBLists.getRight()[groupIdx];
                    // 3. encode bins
                    int groupSize = keyList.size();
                    int mmSketchColNum = (int) Math.ceil(groupSize * mmSketchColRatio);
                    minMaxSketches[groupIdx] = new MinMaxSketch(mmSketchRowNum, mmSketchColNum, zeroValue);
                    for (int j = 0; j < groupSize; j++) {
                        minMaxSketches[groupIdx].insert(keyList.getInt(j), binList.getInt(j));
                    }
                    // 4. encode keys
                    encoders[groupIdx] = new DeltaBinaryEncoder();
                    encoders[groupIdx].encode(keyList.toIntArray(null));
                    return null;
                }
            });
        }
        for (Future<Void> future : futures) {
            future.get();
        }
        LOG.debug(String.format("Sparse vector parallel compression cost %d ms, %d key-value " +
                "pairs in total", System.currentTimeMillis() - startTime, size));
    }

    @Override
    public double[] decompressDense() {
        Pair<int[], double[]> kv = decompressSparse();
        int[] keys = kv.getLeft();
        double[] values = kv.getRight();
        int maxKey = 0;
        for (int key : keys) {
            maxKey = Math.max(key, maxKey);
        }
        double[] res = new double[maxKey + 1];
        for (int i = 0; i < size; i++) {
            res[keys[i]] = values[i];
        }
        return res;
    }

    @Override
    public Pair<int[], double[]> decompressSparse() {
        int[] keys = new int[size];
        double[] values = new double[size];
        int cnt = 0;
        for (int i = 0; i < mmSketchGroupNum; i++) {
            int[] partKeys = encoders[i].decode();
            MinMaxSketch mmSketch = minMaxSketches[i];
            for (int key : partKeys) {
                int binIdx = mmSketch.query(key);
                keys[cnt] = key;
                values[cnt] = quantValues[binIdx];
                cnt++;
            }
        }
        return new ImmutablePair<>(keys, values);
    }

    public Pair<int[], double[]> decompressAndSort() {
        Pair<int[], double[]> res = decompressSparse();
        Sort.quickSort(res.getLeft(), res.getRight(), 0, size - 1);
        return res;
    }

    @Override
    public void timesBy(double x) {
        if (quantValues != null) {
            for (int i = 0; i < quantValues.length; i++)
                quantValues[i] *= x;
        }
    }

    @Override
    public double size() {
        return size;
    }

    @Override
    public int memoryBytes() {
        int res = 28 + quantValues.length * 8;
        if (minMaxSketches != null) {
            for (MinMaxSketch sketch : minMaxSketches) {
                if (sketch != null)
                    res += sketch.memoryBytes();
            }
        }
        if (encoders != null) {
            for (BinaryEncoder encoder : encoders) {
                if (encoder != null)
                    res += encoder.memoryBytes();
            }
        }
        return res;
    }
}
