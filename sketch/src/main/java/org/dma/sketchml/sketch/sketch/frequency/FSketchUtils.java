package org.dma.sketchml.sketch.sketch.frequency;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class FSketchUtils {

    public static int[] calGroupEdges(int zeroIdx, int binNum, int groupNum) {
        if (groupNum == 2) {
            return new int[]{zeroIdx, binNum};
        } else {
            int[] groupEdges = new int[groupNum];
            int binsPerGroup = binNum / groupNum;
            if (zeroIdx < binsPerGroup) {
                groupEdges[0] = zeroIdx;
            } else if ((zeroIdx % binsPerGroup) < (binsPerGroup / 2)) {
                groupEdges[0] = binsPerGroup + zeroIdx % binsPerGroup;
            } else {
                groupEdges[0] = zeroIdx % binsPerGroup;
            }
            for (int i = 1; i < groupNum - 1; i++) {
                groupEdges[i] = groupEdges[i - 1] + binsPerGroup;
            }
            groupEdges[groupNum - 1] = binNum;
            return groupEdges;
        }
    }

    public static Pair<IntArrayList[], IntArrayList[]> partition(int[] keys, int[] bins, int[] groupEdges) {
        int groupNum = groupEdges.length;
        IntArrayList[] keyLists = new IntArrayList[groupNum];
        IntArrayList[] binLists = new IntArrayList[groupNum];
        for (int i = 0; i < groupNum; i++) {
            int groupSpan = i > 0 ? (groupEdges[i] - groupEdges[i - 1]) : groupEdges[0];
            int estimatedGroupSize = (int) Math.ceil(1.0 * keys.length / groupNum * groupSpan);
            keyLists[i] = new IntArrayList(estimatedGroupSize);
            binLists[i] = new IntArrayList(estimatedGroupSize);
        }
        for (int i = 0; i < keys.length; i++) {
            int groupIdx = 0;
            while (groupEdges[groupIdx] <= bins[i]) groupIdx++;
            keyLists[groupIdx].add(keys[i]);
            binLists[groupIdx].add(bins[i]);
        }
        return new ImmutablePair<>(keyLists, binLists);
    }

}
