package org.dma.sketchml.base;

import org.dma.sketchml.common.Constants;

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

    protected byte[] byteBins;
    protected short[] shortBins;
    protected int[] intBins;
    public static final int DEFAULT_BIN_NUM = 256;

    public Quantizer(int binNum) {
        this.binNum = binNum;
    }

    public abstract void quantize(double[] values);

    public abstract void parallelQuantize(double[] values) throws InterruptedException, ExecutionException;

    public double[] getValues() {
        double[] res = new double[binNum];
        int index = 0;
        while (index + 1 < binNum) {
            res[index] = 0.5 * (splits[index] + splits[index + 1]);
            index++;
        }
        res[index] = 0.5 * (splits[index] + max);
        return res;
    }

    public int indexOf(double x) {
        int l = zeroIdx, r = zeroIdx;
        if (x < 0.0)
            l = 0;
        else
            r = binNum - 1;
        while (l <= r) {
            int mid = (l + r) >> 1;
            if (splits[mid] <= x) {
                if (mid + 1 == binNum || splits[mid + 1] > x)
                    return mid;
                else
                    l = mid + 1;
            } else {
                r = mid - 1;
            }
        }
        return zeroIdx;
    }

    protected void findZeroIdx() {
        if (min > 0.0)
            zeroIdx = 0;
        else if (max < 0.0)
            zeroIdx = binNum - 1;
        else {
            int t = 0;
            while (t < binNum - 1 && splits[t + 1] < 0.0)
                t++;
            zeroIdx = t;
        }
    }

    protected void quantizeToBins(double[] values) {
        int size = values.length;
        if (binNum <= 256) {
            byteBins = new byte[size];
            for (int i = 0; i < size; i++) {
                int binIdx = indexOf(values[i]);
                byteBins[i] = (byte) (binIdx + Byte.MIN_VALUE);
            }
        } else if (binNum <= 65536) {
            shortBins = new short[size];
            for (int i = 0; i < size; i++) {
                int binIdx = indexOf(values[i]);
                shortBins[i] = (short) (binIdx + Short.MIN_VALUE);
            }
        } else {
            intBins = new int[size];
            for (int i = 0; i < size; i++) {
                int binIdx = indexOf(values[i]);
                intBins[i] = binIdx + Integer.MIN_VALUE;
            }
        }
    }

    protected void parallelQuantizeToBins(double[] values) throws InterruptedException, ExecutionException {
        int size = values.length;
        int threadNum = Constants.Parallel.getParallelism();
        ExecutorService threadPool = Constants.Parallel.getThreadPool();
        Future<Void>[] futures = new Future[threadNum];
        if (binNum <= 256) {
            byteBins = new byte[size];
            for (int i = 0; i < threadNum; i++) {
                int threadId = i;
                futures[threadId] = threadPool.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        int elementPerThread = n / threadNum;
                        int from = threadId * elementPerThread;
                        int to = threadId + 1 == threadNum ? size : from + elementPerThread;
                        for (int itemId = from; itemId < to; itemId++) {
                            int binIdx = indexOf(values[itemId]);
                            byteBins[itemId] = (byte) (binIdx + Byte.MIN_VALUE);
                        }
                        return null;
                    }
                });
            }
        } else if (binNum <= 65536) {
            shortBins = new short[size];
            for (int i = 0; i < threadNum; i++) {
                int threadId = i;
                futures[threadId] = threadPool.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        int elementPerThread = n / threadNum;
                        int from = threadId * elementPerThread;
                        int to = threadId + 1 == threadNum ? size : from + elementPerThread;
                        for (int itemId = from; itemId < to; itemId++) {
                            int binIdx = indexOf(values[itemId]);
                            shortBins[itemId] = (short) (binIdx + Short.MIN_VALUE);
                        }
                        return null;
                    }
                });
            }
        } else {
            intBins = new int[size];
            for (int i = 0; i < threadNum; i++) {
                int threadId = i;
                futures[threadId] = threadPool.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        int elementPerThread = n / threadNum;
                        int from = threadId * elementPerThread;
                        int to = threadId + 1 == threadNum ? size : from + elementPerThread;
                        for (int itemId = from; itemId < to; itemId++) {
                            int binIdx = indexOf(values[itemId]);
                            intBins[itemId] = binIdx + Integer.MIN_VALUE;
                        }
                        return null;
                    }
                });
            }
        }
        for (int i = 0; i < threadNum; i++) {
            futures[i].get();
        }
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

    public byte[] getByteBins() {
        return byteBins;
    }

    public short[] getShortBins() {
        return shortBins;
    }

    public int[] getIntBins() {
        return intBins;
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
}
