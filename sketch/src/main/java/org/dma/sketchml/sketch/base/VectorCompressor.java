package org.dma.sketchml.sketch.base;

import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ExecutionException;

public interface VectorCompressor extends Serializable {
    void compressDense(double[] values);

    void compressSparse(int[] keys, double[] values);

    void parallelCompressDense(double[] values) throws InterruptedException, ExecutionException;

    void parallelCompressSparse(int[] keys, double[] values) throws InterruptedException, ExecutionException;

    double[] decompressDense();

    Pair<int[], double[]> decompressSparse();

    void timesBy(double x);

    double size();

    int memoryBytes() throws IOException;
}
