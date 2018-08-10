package org.dma.sketchml.sketch.sample;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.dma.sketchml.sketch.base.Quantizer;
import org.dma.sketchml.sketch.base.SketchMLException;
import org.dma.sketchml.sketch.base.VectorCompressor;
import org.dma.sketchml.sketch.util.Maths;
import org.dma.sketchml.sketch.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

public class DenseVectorCompressor implements VectorCompressor {
    private static final Logger LOG = LoggerFactory.getLogger(DenseVectorCompressor.class);

    private int size;

    private Quantizer.QuantizationType quantType;
    private int quantBinNum;
    private Quantizer quantizer;

    public DenseVectorCompressor(
            Quantizer.QuantizationType quantType, int quantBinNum) {
        this.quantType = quantType;
        this.quantBinNum = quantBinNum;
    }

    @Override
    public void compressDense(double[] values) {
        long startTime = System.currentTimeMillis();
        size = values.length;
        quantizer = Quantizer.newQuantizer(quantType, quantBinNum);
        quantizer.quantize(values);
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
        int maxKey = Maths.max(keys);
        double[] dense = new double[maxKey];
        for (int i = 0; i < keys.length; i++)
            dense[keys[i]] = values[i];
        compressDense(dense);
    }

    @Override
    public void parallelCompressDense(double[] values) throws InterruptedException, ExecutionException {
        long startTime = System.currentTimeMillis();
        size = values.length;
        quantizer = Quantizer.newQuantizer(quantType, quantBinNum);
        quantizer.parallelQuantize(values);
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
        int maxKey = Maths.max(keys);
        double[] dense = new double[maxKey];
        for (int i = 0; i < keys.length; i++)
            dense[keys[i]] = values[i];
        parallelCompressDense(dense);
    }

    @Override
    public double[] decompressDense() {
        double[] values = new double[size];
        double[] quantValues = quantizer.getValues();
        int[] bins = quantizer.getBins();
        for (int i = 0; i < size; i++)
            values[i] = quantValues[bins[i]];
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
    }

    @Override
    public double size() {
        return size;
    }

    @Override
    public int memoryBytes() throws IOException {
        int res = 12;
        if (quantizer != null) res += Utils.sizeof(quantizer);
        return res;
    }
}
