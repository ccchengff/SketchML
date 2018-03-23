package org.dma.sketchml.hash;

import org.dma.sketchml.base.Int2IntHash;
import org.dma.sketchml.base.SketchMLException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class HashFactory {
    private static final Int2IntHash[] int2intHashes =
            new Int2IntHash[]{new BJHash(0), new Mix64Hash(0),
            new TWHash(0), new BKDRHash(0, 31), new BKDRHash(0, 131),
            new BKDRHash(0, 267), new BKDRHash(0, 1313), new BKDRHash(0, 13131)};
    private static final Random random = new Random();

    public static Int2IntHash getRandomInt2IntHash(int size) {
        int idx = random.nextInt(int2intHashes.length);
        Int2IntHash res = int2intHashes[idx].clone();
        res.setSize(size);
        return res;
    }

    public static Int2IntHash[] getRandomInt2IntHashes(int hashNum, int size) {
        if (hashNum > int2intHashes.length) {
            throw new SketchMLException(String.format("Currently only %d " +
                    "hash functions are available", int2intHashes.length));
        } else {
            Int2IntHash[] res = new Int2IntHash[hashNum];
            List<Integer> idxes = new ArrayList<>(int2intHashes.length);
            for (int i = 0; i < int2intHashes.length; i++) idxes.add(i);
            Collections.shuffle(idxes, random);
            for (int i = 0; i < hashNum; i++) {
                res[i] = int2intHashes[idxes.get(i)].clone();
                res[i].setSize(size);
            }
            return res;
        }
    }
}
