package org.dma.sketchml.sketch.base;


import java.io.Serializable;

public abstract class QuantileSketch implements Serializable {
    protected long n; // total number of data items appeared
    protected long estimateN; // estimated total number of data items there will be,
    // if not -1, sufficient space will be allocated at once

    protected double minValue;
    protected double maxValue;

    public QuantileSketch(long estimateN) {
        this.estimateN = estimateN > 0 ? estimateN : -1L;
    }

    public QuantileSketch() {
        this(-1L);
    }

    public abstract void reset();

    public abstract void update(double value);

    public abstract void merge(QuantileSketch other);

    public abstract double getQuantile(double fraction);

    public abstract double[] getQuantiles(double[] fractions);

    public abstract double[] getQuantiles(int evenPartition);

    public boolean isEmpty() {
        return n == 0;
    }

    public long getN() {
        return n;
    }

    public long getEstimateN() {
        return estimateN;
    }

    public double getMinValue() {
        return minValue;
    }

    public double getMaxValue() {
        return maxValue;
    }
}
