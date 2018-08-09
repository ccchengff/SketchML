package org.dma.sketchml.sketch.sketch.frequency;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.dma.sketchml.sketch.base.BinaryEncoder;
import org.dma.sketchml.sketch.binary.DeltaBinaryEncoder;
import org.dma.sketchml.sketch.common.Constants;
import org.dma.sketchml.sketch.util.Maths;
import org.dma.sketchml.sketch.util.Sort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class GroupedMinMaxSketch implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(GroupedMinMaxSketch.class);

    private int groupNum;
    private int rowNum;
    private double colRatio;
    private int binNum;
    private int zeroValue;
    private MinMaxSketch[] sketches;
    private BinaryEncoder[] encoders;

    public GroupedMinMaxSketch(int groupNum, int rowNum, double colRatio, int binNum, int zeroValue) {
        this.groupNum = groupNum;
        this.rowNum = rowNum;
        this.colRatio = colRatio;
        this.binNum = binNum;
        this.zeroValue = zeroValue;
    }

    public void create(int[] keys, int[] bins) {
        long startTime = System.currentTimeMillis();
        // 1. divide bins into several groups
        int[] groupEdges = FSketchUtils.calGroupEdges(zeroValue, binNum, groupNum);
        sketches = new MinMaxSketch[groupNum];
        encoders = new BinaryEncoder[groupNum];
        Pair<IntArrayList[], IntArrayList[]> partKBLists =
                FSketchUtils.partition(keys, bins, groupEdges);
        // 2. encode bins and keys
        for (int i = 0; i < groupNum; i++) {
            IntArrayList keyList = partKBLists.getLeft()[i];
            IntArrayList binList = partKBLists.getRight()[i];
            Pair<MinMaxSketch, BinaryEncoder> group = compOneGroup(
                    keyList, binList, groupEdges, i);
            sketches[i] = group.getLeft();
            encoders[i] = group.getRight();
        }
        LOG.debug(String.format("Create grouped MinMaxSketch cost %d ms",
                System.currentTimeMillis() - startTime));
    }

    public void parallelCreate(int[] keys, int[] bins) throws InterruptedException, ExecutionException {
        long startTime = System.currentTimeMillis();
        // 1. divide bins into several groups
        int[] groupEdges = FSketchUtils.calGroupEdges(zeroValue, binNum, groupNum);
        sketches = new MinMaxSketch[groupNum];
        encoders = new BinaryEncoder[groupNum];
        Pair<IntArrayList[], IntArrayList[]> partKBLists =
                FSketchUtils.partition(keys, bins, groupEdges);
        // 2. each thread encode one group of bins and keys
        ExecutorService threadPool = Constants.Parallel.getThreadPool();
        Future<Pair<MinMaxSketch, BinaryEncoder>>[] futures = new Future[groupNum];
        for (int i = 0; i < groupNum; i++) {
            int threadId = i;
            futures[threadId] = threadPool.submit(new Callable<Pair<MinMaxSketch, BinaryEncoder>>() {
                @Override
                public Pair<MinMaxSketch, BinaryEncoder> call() throws Exception {
                    IntArrayList keyList = partKBLists.getLeft()[threadId];
                    IntArrayList binList = partKBLists.getRight()[threadId];
                    return compOneGroup(keyList, binList, groupEdges, threadId);
                }
            });
        }
        for (int i = 0; i < groupNum; i++) {
            Pair<MinMaxSketch, BinaryEncoder> res = futures[i].get();
            sketches[i] = res.getLeft();
            encoders[i] = res.getRight();
        }
        LOG.debug(String.format("Create grouped MinMaxSketch cost %d ms",
                System.currentTimeMillis() - startTime));
    }

    private Pair<MinMaxSketch, BinaryEncoder> compOneGroup(IntArrayList keyList, IntArrayList binList,
                                                           int[] groupEdges, int groupId) {
        // encode bins
        int groupSize = keyList.size();
        int colNum = (int) Math.ceil(groupSize * colRatio);
        int partBinNum = groupEdges[groupId] - (groupId == 0 ? 0 : groupEdges[groupId - 1]);
        int bitsPerCell = Maths.log2nlz(partBinNum);
        MinMaxSketch sketch = new MinMaxSketch(rowNum, colNum, bitsPerCell, zeroValue);
        for (int j = 0; j < groupSize; j++) {
            sketch.insert(keyList.getInt(j), binList.getInt(j));
        }
        // encode keys
        BinaryEncoder encoder = new DeltaBinaryEncoder();
        encoder.encode(keyList.toIntArray(null));
        return new ImmutablePair<>(sketch, encoder);
    }

    public Pair<int[], int[]> restore() {
        int size = 0;
        // decode each group
        int[][] groupKeys = new int[groupNum][];
        int[][] groupBins = new int[groupNum][];
        for (int i = 0; i < groupNum; i++) {
            groupKeys[i] = encoders[i].decode();
            groupBins[i] = new int[groupKeys[i].length];
            for (int j = 0; j < groupKeys[i].length; j++)
                groupBins[i][j] = sketches[i].query(groupKeys[i][j]);
            size += groupKeys[i].length;
        }
        // merge
        int[] keys = new int[size];
        int[] bins = new int[size];
        Sort.merge(groupKeys, groupBins, keys, bins);
        return new ImmutablePair<>(keys, bins);
    }

    public int memoryBytes() {
        int res = 24;
        if (sketches != null) {
            for (int i = 0; i < groupNum; i++)
                res += sketches[i].memoryBytes() + encoders[i].memoryBytes();
        }
        return res;
    }

}
