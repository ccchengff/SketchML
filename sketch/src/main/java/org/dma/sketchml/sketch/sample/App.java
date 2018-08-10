package org.dma.sketchml.sketch.sample;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.commons.lang3.tuple.Pair;
import org.dma.sketchml.sketch.base.QuantileSketch;
import org.dma.sketchml.sketch.base.Quantizer;
import org.dma.sketchml.sketch.base.VectorCompressor;
import org.dma.sketchml.sketch.common.Constants;
import org.dma.sketchml.sketch.sketch.frequency.GroupedMinMaxSketch;
import org.dma.sketchml.sketch.sketch.frequency.MinMaxSketch;
import org.dma.sketchml.sketch.sketch.quantile.HeapQuantileSketch;
import org.dma.sketchml.sketch.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Random;

public class App {
    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    private static Random random = new Random();

    public static void main(String[] args) throws Exception {
        Constants.Parallel.setParallelism(4);
        dense();
        sparse();
        Constants.Parallel.shutdown();
    }

    private static void dense() throws Exception {
        int n = 1000000;
        double density = 0.9;
        double[] values = new double[n];
        for (int i = 0; i < n; i++) {
            if (random.nextDouble() < density) {
                values[i] = random.nextGaussian();
            }
        }
        Quantizer.QuantizationType quantType = Quantizer.QuantizationType.QUANTILE;
        int binNum = Quantizer.DEFAULT_BIN_NUM;
        VectorCompressor compressor = new DenseVectorCompressor(quantType, binNum);
        //compressor.compressDense(values);
        compressor.parallelCompressDense(values);
        compressor = (VectorCompressor) Utils.testSerialization(compressor);
        double[] dValues = compressor.decompressDense();
        LOG.info("First 10 values before: " + Arrays.toString(Arrays.copyOf(values, 10)));
        LOG.info("First 10 values after:  " + Arrays.toString(Arrays.copyOf(dValues, 10)));
        QuantileSketch qSketch = new HeapQuantileSketch((long) n);
        double rmse = 0.0;
        for (int i = 0; i < n; i++) {
            qSketch.update(dValues[i] - values[i]);
            rmse += (dValues[i] - values[i]) *(dValues[i] - values[i]);
        }
        double[] err = qSketch.getQuantiles(100);
        rmse = Math.sqrt(rmse / n);
        LOG.info("Quantiles of errors: " + Arrays.toString(err));
        LOG.info("RMSE: " + rmse);
        int originBytes = values.length * 8;
        int compressBytes = compressor.memoryBytes();
        LOG.info(String.format("Compress %d bytes into %d bytes, compression rate: %f",
                originBytes, compressBytes, 1.0 * originBytes / compressBytes));
    }

    private static void sparse() throws Exception {
        int n = 100000;
        double sparsity = 0.9;
        IntArrayList keyList = new IntArrayList();
        DoubleArrayList valueList = new DoubleArrayList();
        for (int i = 0; i < n; i++) {
            if (random.nextDouble() > sparsity) {
                keyList.add(i);
                valueList.add(random.nextGaussian());
            }
        }
        int nnz = keyList.size();
        int[] keys = keyList.toIntArray();
        double[] values = valueList.toDoubleArray();
        Quantizer.QuantizationType quantType = Quantizer.QuantizationType.QUANTILE;
        int binNum = Quantizer.DEFAULT_BIN_NUM;
        int groupNum = GroupedMinMaxSketch.DEFAULT_MINMAXSKETCH_GROUP_NUM;
        int rowNum = MinMaxSketch.DEFAULT_MINMAXSKETCH_ROW_NUM;
        double colRatio = GroupedMinMaxSketch.DEFAULT_MINMAXSKETCH_COL_RATIO;
        VectorCompressor compressor = new SparseVectorCompressor(
                quantType, binNum, groupNum, rowNum, colRatio);
        compressor = (VectorCompressor) Utils.testSerialization(compressor);
        //compressor.compressSparse(keys, values);
        compressor.parallelCompressSparse(keys, values);
        Pair<int[], double[]> dResult = compressor.decompressSparse();
        int[] dKeys = dResult.getLeft();
        double[] dValues = dResult.getRight();
        LOG.info(String.format("Array length: [%d, %d] vs. [%d, %d]",
                nnz, nnz, dKeys.length, dValues.length));
        LOG.info("First 10 keys before: " + Arrays.toString(Arrays.copyOf(keys, 10)));
        LOG.info("First 10 keys after:  " + Arrays.toString(Arrays.copyOf(dKeys, 10)));
        LOG.info("First 10 values before: " + Arrays.toString(Arrays.copyOf(values, 10)));
        LOG.info("First 10 values after:  " + Arrays.toString(Arrays.copyOf(dValues, 10)));
        QuantileSketch qSketch = new HeapQuantileSketch((long) nnz);
        double rmse = 0.0;
        for (int i = 0; i < nnz; i++) {
            if (keys[i] != dKeys[i]) {
                LOG.error(String.format("Keys not match: [%d, %d]", keys[i], dKeys[i]));
            } else {
                qSketch.update(dValues[i] - values[i]);
                rmse += (dValues[i] - values[i]) * (dValues[i] - values[i]);
            }
        }
        double[] err = qSketch.getQuantiles(100);
        rmse = Math.sqrt(rmse / nnz);
        LOG.info("Quantiles of errors: " + Arrays.toString(err));
        LOG.info("RMSE: " + rmse);
        int originBytes = 12 * nnz;
        int compressBytes = compressor.memoryBytes();
        LOG.info(String.format("Compress %d bytes into %d bytes, compression rate: %f",
                originBytes, compressBytes, 1.0 * originBytes / compressBytes));
    }
}
