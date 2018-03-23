package org.dma.sketchml.quantization;

import org.dma.sketchml.base.Quantizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class UniformQuantizer extends Quantizer {
    public static final Logger LOG = LoggerFactory.getLogger(UniformQuantizer.class);

    public UniformQuantizer(int binNum) {
        super(binNum);
    }

    public UniformQuantizer() {
        super(Quantizer.DEFAULT_BIN_NUM);
    }

    @Override
    public void quantize(double[] values) {
        long startTime = System.currentTimeMillis();
        n = values.length;
        min = Double.MAX_VALUE;
        max = Double.MIN_VALUE;
        for (double v : values) {
            if (v < min) min = v;
            if (v > max) max = v;
        }
        // 1. uniformly split the range of values
        double step = (max - min) / binNum;
        splits = new double[binNum];
        splits[0] = min;
        for (int i = 0; i < binNum; i++) {
            splits[i] = splits[i - 1] + step;
        }
        // 3. find the zero index
        findZeroIdx();
        // 4. find index of each value
        quantizeToBins(values);
        LOG.debug(String.format("Uniform quantization for %d items cost %d ms",
                n, System.currentTimeMillis() - startTime));
    }

    @Override
    public void parallelQuantize(double[] values) throws InterruptedException, ExecutionException {
        long startTime = System.currentTimeMillis();
        n = values.length;
        min = Double.MAX_VALUE;
        max = Double.MIN_VALUE;
        for (double v : values) {
            if (v < min) min = v;
            if (v > max) max = v;
        }
        // 1. uniformly split the range of values
        double step = (max - min) / binNum;
        splits = new double[binNum];
        splits[0] = min;
        for (int i = 0; i < binNum; i++) {
            splits[i] = splits[i - 1] + step;
        }
        // 3. find the zero index
        findZeroIdx();
        // 4. find index of each value
        parallelQuantizeToBins(values);
        LOG.debug(String.format("Uniform quantization for %d items cost %d ms",
                n, System.currentTimeMillis() - startTime));
    }

    @Override
    public QuantizationType quantizationType() {
        return QuantizationType.UNIFORM;
    }

}
