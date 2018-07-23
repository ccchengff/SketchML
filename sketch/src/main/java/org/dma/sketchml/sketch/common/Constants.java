package org.dma.sketchml.sketch.common;

import org.dma.sketchml.sketch.base.SketchMLException;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Constants {
    public static class Parallel {
        private static int parallelism;

        private static ExecutorService threadPool;

        static {
            parallelism = 0;
            threadPool = null;
        }

        public static void setParallelism(int parallelism) {
            if (parallelism < 1)
                throw new SketchMLException("Invalid parallelism: " + parallelism);
            Parallel.parallelism = parallelism;
            Parallel.threadPool = Executors.newFixedThreadPool(parallelism);
        }

        public static int getParallelism() {
            if (parallelism <= 0)
                throw new SketchMLException("Parallelism is not set yet");
            return parallelism;
        }

        public static ExecutorService getThreadPool() {
            if (threadPool == null)
                throw new SketchMLException("Parallelism is not set yet");
            return threadPool;
        }

        public static void shutdown() {
            if (threadPool != null)
                threadPool.shutdown();
        }
    }

}
