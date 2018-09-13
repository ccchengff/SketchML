package org.dma.sketchml.sketch.binary;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import org.dma.sketchml.sketch.base.BinaryEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.stream.IntStream;

public class HuffmanEncoder implements BinaryEncoder {
    private static final Logger LOG = LoggerFactory.getLogger(HuffmanEncoder.class);

    private Item[] items;
    private BitSet bitset;
    private int size;


    private class Node {
        int value;
        int occurrence;
        Node leftChild;
        Node rightChild;
        boolean isLeaf;

        Node(int value, int occurrence, Node leftChild, Node rightChild, boolean isLeaf) {
            this.value = value;
            this.occurrence = occurrence;
            this.leftChild = leftChild;
            this.rightChild = rightChild;
            this.isLeaf = isLeaf;
        }

        Node(int value, int occurrence) {
            this(value, occurrence, null, null, false);
        }

        Node() {
            this(-1, -1, null, null, false);
        }

        int getValue() {
            return value;
        }

        int getOccurrence() {
            return occurrence;
        }
    }

    private class Item {
        int value;
        int bits;
        int numBits;

        Item(int value, int bits, int numBits) {
            this.value = value;
            this.bits = bits;
            this.numBits = numBits;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("(");
            sb.append(value);
            sb.append(" --> ");
            sb.append(BinaryUtils.bits2String(bits, numBits));
            sb.append(")");
            return sb.toString();
        }
    }

    private void traverse(Node node, Int2ObjectMap<Item> mapping, int bits, int depth) {
        if (node.isLeaf) {
            mapping.put(node.value, new Item(node.value, bits, depth == 0 ? 1 : depth));
        } else {
            traverse(node.leftChild, mapping, bits << 1, depth + 1);
            traverse(node.rightChild, mapping, (bits << 1) | 1, depth + 1);
        }
    }

    @Override
    public void encode(int[] values) {
        long startTime = System.currentTimeMillis();
        // 1. count occurrences
        Int2ObjectRBTreeMap<Node> freq = new Int2ObjectRBTreeMap<>();
        for (int v : values) {
            Node node = freq.get(v);
            if (node != null)
                node.occurrence++;
            else
                freq.put(v, new Node(v, 1, null, null, true));
        }
        // 2. build tree
        PriorityQueue<Node> heap = new PriorityQueue<>(freq.size(),
                Comparator.comparing(Node::getOccurrence));
        heap.addAll(freq.values());
        while (heap.size() > 1) {
            Node x = heap.poll();
            Node y = heap.poll();
            Node p = new Node(-1, x.occurrence + y.occurrence, x, y, false);
            heap.add(p);
        }
        Int2ObjectMap<Item> mapping = new Int2ObjectRBTreeMap<>();
        traverse(heap.peek(), mapping, 0, 0);
        items = new Item[mapping.size()];
        mapping.values().toArray(items);
        // 3. encode values
        bitset = new BitSet();
        int offset = 0;
        for (int v : values) {
            Item item = mapping.get(v);
            BinaryUtils.setBits(bitset, offset, item.bits, item.numBits);
            offset += item.numBits;
        }
        size = values.length;
        LOG.debug(String.format("Huffman encoding for %d values cost %d ms",
                values.length, System.currentTimeMillis() - startTime));
    }

    @Override
    public int[] decode() {
        if (size == 0)
            return new int[0];

        // 1. build Huffman tree
        Node root = new Node();
        for (Item item : items) {
            int bits = item.bits;
            int numBits = item.numBits;
            Node cur = root;
            for (int i = numBits - 1; i >= 0; i--) {
                int t = bits - (1 << i);
                if (t >= 0) {
                    if (cur.rightChild == null)
                        cur.rightChild = new Node();
                    cur = cur.rightChild;
                    bits = t;
                } else {
                    if (cur.leftChild == null)
                        cur.leftChild = new Node();
                    cur = cur.leftChild;
                }
            }
            cur.value = item.value;
            cur.isLeaf = true;
        }
        // 2. decode bits
        int[] res = new int[size];
        int cnt = 0;
        Node cur = root;
        int idx = 0;
        while (cnt < size) {
            cur = bitset.get(idx++) ? cur.rightChild : cur.leftChild;
            if (cur.isLeaf) {
                res[cnt++] = cur.value;
                cur = root;
            }
        }
        return res;
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
        // items
        if (items == null) {
            oos.writeInt(0);
        } else {
            oos.writeInt(items.length);
            for (Item item : items) {
                oos.writeInt(item.value);
                oos.writeInt(item.bits);
                oos.writeInt(item.numBits);
            }
        }
        // bit set
        if (bitset == null) {
            oos.writeInt(0);
        } else {
            long[] bits = bitset.toLongArray();
            oos.writeInt(bits.length);
            for (long l : bits)
                oos.writeLong(l);
        }
        // size
        oos.writeInt(size);
    }

    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        // items
        int numItems = ois.readInt();
        items = new Item[numItems];
        for (int i = 0; i < numItems; i++)
            items[i] = new Item(ois.readInt(), ois.readInt(), ois.readInt());
        // bit set
        int numLongs = ois.readInt();
        long[] bits = new long[numLongs];
        for (int i = 0; i < numLongs; i++)
            bits[i] = ois.readLong();
        bitset = BitSet.valueOf(bits);
        // size
        size = ois.readInt();
    }
}
