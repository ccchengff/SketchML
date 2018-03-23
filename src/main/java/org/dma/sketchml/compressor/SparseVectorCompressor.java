package org.dma.sketchml.compressor;


import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.dma.sketchml.Util.Sort;
import org.dma.sketchml.base.*;
import org.dma.sketchml.common.Constants;
import org.dma.sketchml.sketch.frequency.*;
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
        if (quantBinNum <= 256) {
            Pair<IntArrayList[], ByteArrayList[]> partKBLists = CompressUtil.partition(
                    keys, quantizer.getByteBins(), groupEdges);
            byte zeroValue = (byte) (quantizer.getZeroIdx() + Byte.MIN_VALUE);
            for (int i = 0; i < mmSketchGroupNum; i++) {
                minMaxSketches[i] = CompressUtil.encodeByteBins(partKBLists.getLeft()[i], partKBLists.getRight()[i],
                        zeroValue, mmSketchRowNum, mmSketchColRatio);
                encoders[i] = CompressUtil.encodeKeys(partKBLists.getLeft()[i]);
            }
        } else if (quantBinNum <= 65536) {
            Pair<IntArrayList[], ShortArrayList[]> partKBLists = CompressUtil.partition(
                    keys, quantizer.getShortBins(), groupEdges);
            short zeroValue = (short) (quantizer.getZeroIdx() + Short.MIN_VALUE);
            for (int i = 0; i < mmSketchGroupNum; i++) {
                minMaxSketches[i] = CompressUtil.encodeShortBins(partKBLists.getLeft()[i], partKBLists.getRight()[i],
                        zeroValue, mmSketchRowNum, mmSketchColRatio);
                encoders[i] = CompressUtil.encodeKeys(partKBLists.getLeft()[i]);
            }
        } else {
            Pair<IntArrayList[], IntArrayList[]> partKBLists = CompressUtil.partition(
                    keys, quantizer.getIntBins(), groupEdges);
            int zeroValue = quantizer.getZeroIdx() + Integer.MIN_VALUE;
            for (int i = 0; i < mmSketchGroupNum; i++) {
                minMaxSketches[i] = CompressUtil.encodeIntBins(partKBLists.getLeft()[i], partKBLists.getRight()[i],
                        zeroValue, mmSketchRowNum, mmSketchColRatio);
                encoders[i] = CompressUtil.encodeKeys(partKBLists.getLeft()[i]);
            }
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
        if (quantBinNum <= 256) {
            Pair<IntArrayList[], ByteArrayList[]> partKBLists = CompressUtil.partition(
                    keys, quantizer.getByteBins(), groupEdges);
            byte zeroValue = (byte) (quantizer.getZeroIdx() + Byte.MIN_VALUE);
            for (int i = 0; i < mmSketchGroupNum; i++) {
                int groupIdx = i;
                futures[i] = threadPool.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        minMaxSketches[groupIdx] = CompressUtil.encodeByteBins(
                                partKBLists.getLeft()[groupIdx], partKBLists.getRight()[groupIdx],
                                zeroValue, mmSketchRowNum, mmSketchColRatio);
                        encoders[groupIdx] = CompressUtil.encodeKeys(partKBLists.getLeft()[groupIdx]);
                        return null;
                    }
                });
            }
        } else if (quantBinNum <= 65536) {
            Pair<IntArrayList[], ShortArrayList[]> partKBLists = CompressUtil.partition(
                    keys, quantizer.getShortBins(), groupEdges);
            short zeroValue = (short) (quantizer.getZeroIdx() + Short.MIN_VALUE);
            for (int i = 0; i < mmSketchGroupNum; i++) {
                int groupIdx = i;
                futures[i] = threadPool.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        minMaxSketches[groupIdx] = CompressUtil.encodeShortBins(
                                partKBLists.getLeft()[groupIdx], partKBLists.getRight()[groupIdx],
                                zeroValue, mmSketchRowNum, mmSketchColRatio);
                        encoders[groupIdx] = CompressUtil.encodeKeys(partKBLists.getLeft()[groupIdx]);
                        return null;
                    }
                });
            }
        } else {
            Pair<IntArrayList[], IntArrayList[]> partKBLists = CompressUtil.partition(
                    keys, quantizer.getIntBins(), groupEdges);
            int zeroValue = quantizer.getZeroIdx() + Integer.MIN_VALUE;
            for (int i = 0; i < mmSketchGroupNum; i++) {
                int groupIdx = i;
                futures[i] = threadPool.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        minMaxSketches[groupIdx] = CompressUtil.encodeIntBins(
                                partKBLists.getLeft()[groupIdx], partKBLists.getRight()[groupIdx],
                                zeroValue, mmSketchRowNum, mmSketchColRatio);
                        encoders[groupIdx] = CompressUtil.encodeKeys(partKBLists.getLeft()[groupIdx]);
                        return null;
                    }
                });
            }
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
            if (quantBinNum <= 256) {
                ByteMinMaxSketch minMaxSketch = (ByteMinMaxSketch) minMaxSketches[i];
                for (int key : partKeys) {
                    int binIdx = ((int) minMaxSketch.qurey(key)) - Byte.MIN_VALUE;
                    keys[cnt] = key;
                    values[cnt] = quantValues[binIdx];
                    cnt++;
                }
            } else if (quantBinNum <= 65536) {
                ShortMinMaxSketch minMaxSketch = (ShortMinMaxSketch) minMaxSketches[i];
                for (int key : partKeys) {
                    int binIdx = ((int) minMaxSketch.qurey(key)) - Short.MIN_VALUE;
                    keys[cnt] = key;
                    values[cnt] = quantValues[binIdx];
                    cnt++;
                }
            } else {
                IntMinMaxSketch minMaxSketch = (IntMinMaxSketch) minMaxSketches[i];
                for (int key : partKeys) {
                    int binIdx = minMaxSketch.qurey(key) - Integer.MIN_VALUE;
                    keys[cnt] = key;
                    values[cnt] = quantValues[binIdx];
                    cnt++;
                }
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
