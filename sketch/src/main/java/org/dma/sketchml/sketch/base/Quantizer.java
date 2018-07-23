package org.dma.sketchml.sketch.base;

import org.dma.sketchml.sketch.common.Constants;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public abstract class Quantizer implements Serializable {
    protected int binNum;
    protected int n;
    protected double[] splits;
    protected int zeroIdx;
    protected double min;
    protected double max;

    protected int[] bins;
    public static final int DEFAULT_BIN_NUM = 256;

    public Quantizer(int binNum) {
        this.binNum = binNum;
    }

    public abstract void quantize(double[] values);

    public abstract void parallelQuantize(double[] values) throws InterruptedException, ExecutionException;

    public double[] getValues() {
        double[] res = new double[binNum];
        int splitNum = binNum - 1;
        res[0] = 0.5 * (min + splits[0]);
        for (int i = 1; i < splitNum; i++)
            res[i] = 0.5 * (splits[i - 1] + splits[i]);
        res[splitNum] = 0.5 * (splits[splitNum - 1] + max);
        return res;
    }

    public int indexOf(double x) {
        if (x < splits[0]) {
            return 0;
        } else if (x > splits[binNum - 2]) {
            return binNum - 1;
        } else {
            int l = zeroIdx, r = zeroIdx;
            if (x < 0.0) l = 0;
            else r = binNum - 2;
            while (l + 1 < r) {
                int mid = (l + r) >> 1;
                if (splits[mid] > x) {
                    if (mid == 0 || splits[mid - 1] <= x)
                        return mid + 1;
                    else
                        r = mid;
                } else {
                    l = mid;
                }
            }
            int mid = (l + r) >> 1;
            return splits[mid] <= x ? mid + 1 : mid;
        }
    }

    protected void findZeroIdx() {
        if (min > 0.0)
            zeroIdx = 0;
        else if (max < 0.0)
            zeroIdx = binNum - 1;
        else {
            int t = 0;
            while (t < binNum - 1 && splits[t] < 0.0)
                t++;
            zeroIdx = t;
        }
    }

    protected void quantizeToBins(double[] values) {
        int size = values.length;
        bins = new int[size];
        for (int i = 0; i < size; i++)
            bins[i] = indexOf(values[i]);
    }

    protected void parallelQuantizeToBins(double[] values) throws InterruptedException, ExecutionException {
        int size = values.length;
        int threadNum = Constants.Parallel.getParallelism();
        ExecutorService threadPool = Constants.Parallel.getThreadPool();
        Future<Void>[] futures = new Future[threadNum];
        bins = new int[size];
        for (int i = 0; i < threadNum; i++) {
            int threadId = i;
            futures[threadId] = threadPool.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    int elementPerThread = n / threadNum;
                    int from = threadId * elementPerThread;
                    int to = threadId + 1 == threadNum ? size : from + elementPerThread;
                    for (int itemId = from; itemId < to; itemId++)
                        bins[itemId] = indexOf(values[itemId]);
                    return null;
                }
            });
        }
        for (int i = 0; i < threadNum; i++) {
            futures[i].get();
        }
    }

    public void timesBy(double x) {
        min *= x;
        max *= x;
        for (int i = 0; i < splits.length; i++)
            splits[i] *= x;
    }

    public enum QuantizationType {
        UNIFORM("UNIFORM"),
        QUANTILE("QUANTILE");

        private final String type;

        QuantizationType(String type) {
            this.type = type;
        }

        @Override
        public String toString() {
            return type;
        }
    }

    public abstract QuantizationType quantizationType();

    public int getBinNum() {
        return binNum;
    }

    public int getN() {
        return n;
    }

    public double[] getSplits() {
        return splits;
    }

    public int[] getBins() {
        return bins;
    }

    public int getZeroIdx() {
        return zeroIdx;
    }

    public double getMax() {
        return max;
    }

    public double getMin() {
        return min;
    }

    public int memoryBytes() {
        int memPerBin = binNum <= 256 ? 1 : binNum <= 65536 ? 2 : 4;
        return 28 + splits.length * 8 + bins.length * memPerBin;
    }
}
