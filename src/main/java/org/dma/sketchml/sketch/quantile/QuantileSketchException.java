package org.dma.sketchml.sketch.quantile;

import org.dma.sketchml.base.SketchMLException;

public class QuantileSketchException extends SketchMLException {
    public QuantileSketchException(String message) {
        super(message);
    }

    public QuantileSketchException(Throwable cause) {
        super(cause);
    }

    public QuantileSketchException(String message, Throwable cause) {
        super(message, cause);
    }
}
