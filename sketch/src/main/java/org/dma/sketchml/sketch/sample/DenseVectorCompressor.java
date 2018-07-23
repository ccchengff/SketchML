package org.dma.sketchml.sketch.sample;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.dma.sketchml.sketch.base.Quantizer;
import org.dma.sketchml.sketch.base.SketchMLException;
import org.dma.sketchml.sketch.base.VectorCompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

public class DenseVectorCompressor implements VectorCompressor, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(DenseVectorCompressor.class);

    private int size;

    private Quantizer.QuantizationType quantType;
    private int quantBinNum;
    private Quantizer quantizer;
    //private double[] quantValues;
    //private byte[] byteBins;
    //private short[] shortBins;
    //private int[] intBins;

    public DenseVectorCompressor(
            Quantizer.QuantizationType quantType, int quantBinNum) {
        this.quantType = quantType;
        this.quantBinNum = quantBinNum;
    }

    @Override
    public void compressDense(double[] values) {
        long startTime = System.currentTimeMillis();
        size = values.length;
        // 1. quantize into bin indexes
        quantizer = CompressUtil.newQuantizer(quantType, quantBinNum);
        quantizer.quantize(values);
        //quantValues = quantizer.getValues();
        // TODO: binary encoding on bin indexes
        //if (quantBinNum <= 256) {
        //    byteBins = quantizer.getByteBins();
        //} else if (quantBinNum <= 65536) {
        //    shortBins = quantizer.getShortBins();
        //} else {
        //    intBins = quantizer.getIntBins();
        //}
        LOG.debug(String.format("Dense vector compression cost %d ms, %d items " +
                "in total", System.currentTimeMillis() - startTime, size));
    }

    @Override
    public void compressSparse(int[] keys, double[] values) {
        LOG.warn("Compressing a sparse vector with DenseVectorCompressor");
        if (keys.length != values.length) {
            throw new SketchMLException(String.format(
                    "Lengths of key array and value array do not match: %d, %d",
                    keys.length, values.length));
        }
        int maxKey = 0;
        for (int key : keys) {
            maxKey = Math.max(key, maxKey);
        }
        double[] dense = new double[maxKey];
        for (int i = 0; i < keys.length; i++) {
            dense[keys[i]] = values[i];
        }
        compressDense(dense);
    }

    @Override
    public void parallelCompressDense(double[] values) throws InterruptedException, ExecutionException {
        long startTime = System.currentTimeMillis();
        size = values.length;
        // 1. quantize into bin indexes
        quantizer = CompressUtil.newQuantizer(quantType, quantBinNum);
        quantizer.parallelQuantize(values);
        //quantValues = quantizer.getValues();
        // TODO: binary encoding on bin indexes
        //if (quantBinNum <= 256) {
        //    byteBins = quantizer.getByteBins();
        //} else if (quantBinNum <= 65536) {
        //    shortBins = quantizer.getShortBins();
        //} else {
        //    intBins = quantizer.getIntBins();
        //}
        LOG.debug(String.format("Dense vector parallel compression cost %d ms, %d items " +
                "in total", System.currentTimeMillis() - startTime, size));
    }

    @Override
    public void parallelCompressSparse(int[] keys, double[] values) throws InterruptedException, ExecutionException {
        LOG.warn("Compressing a sparse vector with DenseVectorCompressor");
        if (keys.length != values.length) {
            throw new SketchMLException(String.format(
                    "Lengths of key array and value array do not match: %d, %d",
                    keys.length, values.length));
        }
        int maxKey = 0;
        for (int key : keys) {
            maxKey = Math.max(key, maxKey);
        }
        double[] dense = new double[maxKey];
        for (int i = 0; i < keys.length; i++) {
            dense[keys[i]] = values[i];
        }
        parallelCompressDense(dense);
    }

    @Override
    public double[] decompressDense() {
        double[] values = new double[size];
        double[] quantValues = quantizer.getValues();
        int[] bins = quantizer.getBins();
        for (int i = 0; i < size; i++) {
            values[i] = quantValues[bins[i]];
        }
        //if (quantBinNum <= 256) {
        //    for (byte bin : byteBins) {
        //        int binIdx = ((int) bin) - Byte.MIN_VALUE;
        //        values[cnt++] = quantValues[binIdx];
        //    }
        //} else if (quantBinNum <= 65536) {
        //    for (short bin : shortBins) {
        //        int binIdx = ((int) bin) - Short.MIN_VALUE;
        //        values[cnt++] = quantValues[binIdx];
        //    }
        //} else {
        //    for (int bin : intBins) {
        //        int binIdx = bin - Integer.MIN_VALUE;
        //        values[cnt++] = quantValues[binIdx];
        //    }
        //}
        return values;
    }

    @Override
    public Pair<int[], double[]> decompressSparse() {
        double[] values = decompressDense();
        int[] keys = new int[values.length];
        Arrays.setAll(keys, i -> i);
        return new ImmutablePair<>(keys, values);
    }

    @Override
    public void timesBy(double x) {
        quantizer.timesBy(x);
        //if (quantValues != null) {
        //    for (int i = 0; i < quantValues.length; i++)
        //        quantValues[i] *= x;
        //}
    }

    @Override
    public double size() {
        return size;
    }

    @Override
    public int memoryBytes() {
        int res = 12;
        res += quantizer.memoryBytes();
        //if (quantValues != null) res += 8 * quantValues.length;
        //if (byteBins != null) res += byteBins.length;
        //if (shortBins != null) res += 2 * shortBins.length;
        //if (intBins != null) res += 4 * intBins.length;
        return res;
    }
}
