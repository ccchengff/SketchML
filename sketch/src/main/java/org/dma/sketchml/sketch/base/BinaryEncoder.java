package org.dma.sketchml.sketch.base;

import java.io.Serializable;
import java.util.stream.IntStream;

public interface BinaryEncoder extends Serializable {
    void encode(int[] values);

    void encode(IntStream stream);

    int[] decode();

}
