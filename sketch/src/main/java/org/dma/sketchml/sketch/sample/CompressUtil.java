package org.dma.sketchml.sketch.sample;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.dma.sketchml.sketch.base.BinaryEncoder;
import org.dma.sketchml.sketch.base.Quantizer;
import org.dma.sketchml.sketch.base.SketchMLException;
import org.dma.sketchml.sketch.binary.DeltaBinaryEncoder;
import org.dma.sketchml.sketch.quantization.QuantileQuantizer;
import org.dma.sketchml.sketch.quantization.UniformQuantizer;

public abstract class CompressUtil {
    public static Quantizer newQuantizer(Quantizer.QuantizationType type, int binNum) {
        switch (type) {
            case QUANTILE:
                return new QuantileQuantizer(binNum);
            case UNIFORM:
                return new UniformQuantizer(binNum);
            default:
                throw new SketchMLException(
                        "Unrecognizable quantization type: " + type);
        }
    }
}
