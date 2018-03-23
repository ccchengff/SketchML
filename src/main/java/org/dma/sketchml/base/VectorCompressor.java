package org.dma.sketchml.base;

import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.ExecutionException;

public interface VectorCompressor {
    void compressDense(double[] values);

    void compressSparse(int[] keys, double[] values);

    void parallelCompressDense(double[] values) throws InterruptedException, ExecutionException;

    void parallelCompressSparse(int[] keys, double[] values) throws InterruptedException, ExecutionException;

    double[] decompressDense();

    Pair<int[], double[]> decompressSparse();

    void timesBy(double x);

    double size();

    int memoryBytes();
}
