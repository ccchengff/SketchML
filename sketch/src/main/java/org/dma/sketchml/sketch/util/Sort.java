package org.dma.sketchml.sketch.util;

import it.unimi.dsi.fastutil.doubles.DoubleArrayPriorityQueue;
import it.unimi.dsi.fastutil.doubles.DoubleComparator;
import it.unimi.dsi.fastutil.doubles.DoublePriorityQueue;
import it.unimi.dsi.fastutil.ints.IntComparator;
import org.dma.sketchml.sketch.base.SketchMLException;

/**
 * Quick sort utils
 */
public class Sort {
    public static int quickSelect(int[] array, int k, int low, int high) {
        if (k > 0 && k <= high - low + 1) {
            int pivot = array[high];
            int ii = low;
            for (int jj = low; jj < high; jj++) {
                if (array[jj] <= pivot) {
                    swap(array, ii++, jj);
                }
            }
            swap(array, ii, high);

            if (ii - low == k - 1) {
                return array[ii];
            } else if (ii - low > k - 1) {
                return quickSelect(array, k, low, ii - 1);
            } else {
                return quickSelect(array, k - ii + low - 1, ii + 1, high);
            }
        }
        throw new SketchMLException("k is more than number of elements in array");
    }

    public static double selectKthLargest(double[] array, int k) {
        return selectKthLargest(array, k, new DoubleArrayPriorityQueue(k));
    }

    public static double selectKthLargest(double[] array, int k, DoubleComparator comp) {
        return selectKthLargest(array, k, new DoubleArrayPriorityQueue(k, comp));
    }

    private static double selectKthLargest(double[] array, int k, DoublePriorityQueue queue) {
        if (k > array.length)
            throw new SketchMLException("k is more than number of elements in array");

        int i = 0;
        while (i < k)
            queue.enqueue(array[i++]);
        for (; i < array.length; i++) {
            double top = queue.firstDouble();
            if (array[i] > top) {
                queue.dequeueDouble();
                queue.enqueue(array[i]);
            }
        }
        return queue.firstDouble();
    }

    public static void quickSort(int[] array, double[] values, int low, int high) {
        if (low < high) {
            int tmp = array[low];
            double tmpValue = values[low];
            int ii = low, jj = high;
            while (ii < jj) {
                while (ii < jj && array[jj] >= tmp) {
                    jj--;
                }

                array[ii] = array[jj];
                values[ii] = values[jj];

                while (ii < jj && array[ii] <= tmp) {
                    ii++;
                }

                array[jj] = array[ii];
                values[jj] = values[ii];
            }
            array[ii] = tmp;
            values[ii] = tmpValue;

            quickSort(array, values, low, ii - 1);
            quickSort(array, values, ii + 1, high);
        }
    }

    public static void quickSort(long[] array, double[] values, int low, int high) {
        if (low < high) {
            long tmp = array[low];
            double tmpValue = values[low];
            int ii = low, jj = high;
            while (ii < jj) {
                while (ii < jj && array[jj] >= tmp) {
                    jj--;
                }

                array[ii] = array[jj];
                values[ii] = values[jj];

                while (ii < jj && array[ii] <= tmp) {
                    ii++;
                }

                array[jj] = array[ii];
                values[jj] = values[ii];
            }
            array[ii] = tmp;
            values[ii] = tmpValue;

            quickSort(array, values, low, ii - 1);
            quickSort(array, values, ii + 1, high);
        }
    }

    public static void quickSort(long[] array, int low, int high) {
        if (low < high) {
            long tmp = array[low];
            int ii = low, jj = high;
            while (ii < jj) {
                while (ii < jj && array[jj] >= tmp) {
                    jj--;
                }

                array[ii] = array[jj];

                while (ii < jj && array[ii] <= tmp) {
                    ii++;
                }

                array[jj] = array[ii];
            }
            array[ii] = tmp;

            quickSort(array, low, ii - 1);
            quickSort(array,  ii + 1, high);
        }
    }

    public static void quickSort(int[] array, int[] values, int low, int high) {
        if (low < high) {
            int tmp = array[low];
            int tmpValue = values[low];
            int ii = low, jj = high;
            while (ii < jj) {
                while (ii < jj && array[jj] >= tmp) {
                    jj--;
                }

                array[ii] = array[jj];
                values[ii] = values[jj];

                while (ii < jj && array[ii] <= tmp) {
                    ii++;
                }

                array[jj] = array[ii];
                values[jj] = values[ii];
            }
            array[ii] = tmp;
            values[ii] = tmpValue;

            quickSort(array, values, low, ii - 1);
            quickSort(array, values, ii + 1, high);
        }
    }

    public static void quickSort(double[] x, double[] y, int from, int to, DoubleComparator comp) {
        int len = to - from;
        if (len < 7) {
            selectionSort(x, y, from, to, comp);
        } else {
            int m = from + len / 2;
            int v;
            int a;
            int b;
            if (len > 7) {
                v = from;
                a = to - 1;
                if (len > 50) {
                    b = len / 8;
                    v = med3(x, from, from + b, from + 2 * b, comp);
                    m = med3(x, m - b, m, m + b, comp);
                    a = med3(x, a - 2 * b, a - b, a, comp);
                }

                m = med3(x, v, m, a, comp);
            }

            double seed = x[m];
            a = from;
            b = from;
            int c = to - 1;
            int d = c;

            while (true) {
                int s;
                while (b > c || (s = comp.compare(x[b], seed)) > 0) {
                    for (; c >= b && (s = comp.compare(x[c], seed)) >= 0; --c) {
                        if (s == 0) {
                            swap(x, c, d);
                            swap(y, c, d);
                            d--;
                        }
                    }

                    if (b > c) {
                        s = Math.min(a - from, b - a);
                        vecSwap(x, from, b - s, s);
                        vecSwap(y, from, b - s, s);
                        s = Math.min(d - c, to - d - 1);
                        vecSwap(x, b, to - s, s);
                        vecSwap(y, b, to - s, s);
                        if ((s = b - a) > 1) {
                            quickSort(x, y, from, from + s, comp);
                        }

                        if ((s = d - c) > 1) {
                            quickSort(x, y, to - s, to, comp);
                        }

                        return;
                    }

                    swap(x, b, c);
                    swap(y, b, c);
                    b++;
                    c--;
                }

                if (s == 0) {
                    swap(x, a, b);
                    swap(y, a, b);
                    a++;
                }

                ++b;
            }
        }
    }

    public static void quickSort(double[] x, double[] y, int from, int to) {
        DoubleComparator cmp = new DoubleComparator() {
            public int compare(double v, double v1) {
                if (Math.abs(v - v1) < 10e-12)
                    return 0;
                else
                    return v - v1 > 10e-12 ? 1 : -1;
            }

            public int compare(Double o1, Double o2) {
                if (Math.abs(o1 - o2) < 10e-12)
                    return 0;
                else
                    return o1 - o2 > 10e-12 ? 1 : -1;
            }
        };
        quickSort(x, y, from, to, cmp);
    }

    public static void quickSort(int[] array, float[] values, int low, int high) {
        if (low < high) {
            int tmp = array[low];
            float tmpValue = values[low];
            int ii = low, jj = high;
            while (ii < jj) {
                while (ii < jj && array[jj] >= tmp) {
                    jj--;
                }

                array[ii] = array[jj];
                values[ii] = values[jj];

                while (ii < jj && array[ii] <= tmp) {
                    ii++;
                }

                array[jj] = array[ii];
                values[jj] = values[ii];
            }
            array[ii] = tmp;
            values[ii] = tmpValue;

            quickSort(array, values, low, ii - 1);
            quickSort(array, values, ii + 1, high);
        }
    }


    private static int med3(double[] x, int a, int b, int c, DoubleComparator comp) {
        int ab = comp.compare(x[a], x[b]);
        int ac = comp.compare(x[a], x[c]);
        int bc = comp.compare(x[b], x[c]);
        return ab < 0 ? (bc < 0 ? b : (ac < 0 ? c : a)) : (bc > 0 ? b : (ac > 0 ? c : a));
    }

    private static void vecSwap(double[] x, int a, int b, int n) {
        for (int i = 0; i < n; ++b) {
            swap(x, a, b);
            ++i;
            ++a;
        }

    }

    private static void swap(int[] x, int a, int b) {
        int t = x[a];
        x[a] = x[b];
        x[b] = t;
    }

    private static void swap(double[] x, int a, int b) {
        double t = x[a];
        x[a] = x[b];
        x[b] = t;
    }

    public static void selectionSort(int[] a, int[] y, int from, int to, IntComparator comp) {
        for (int i = from; i < to - 1; ++i) {
            int m = i;

            int u;
            for (u = i + 1; u < to; ++u) {
                if (comp.compare(a[u], a[m]) < 0) {
                    m = u;
                }
            }

            if (m != i) {
                u = a[i];
                a[i] = a[m];
                a[m] = u;
                u = y[i];
                y[i] = y[m];
                y[m] = u;
            }
        }

    }

    public static void selectionSort(double[] a, double[] y, int from, int to,
                                     DoubleComparator comp) {
        for (int i = from; i < to - 1; ++i) {
            int m = i;
            for (int u = i + 1; u < to; ++u) {
                if (comp.compare(a[u], a[m]) < 0) {
                    m = u;
                }
            }

            if (m != i) {
                double temp = a[i];
                a[i] = a[m];
                a[m] = temp;
                temp = y[i];
                y[i] = y[m];
                y[m] = temp;
            }
        }
    }

    public static void merge(int[][] as, int[][] ys, int[] a, int[] y) {
        int[] ks = new int[as.length];
        int cur = 0;
        while (cur < a.length) {
            int argmin = -1;
            int min = Integer.MAX_VALUE;
            for (int i = 0; i < ks.length; i++) {
                if (ks[i] < as[i].length && as[i][ks[i]] < min) {
                    argmin = i;
                    min = as[i][ks[i]];
                }
            }
            a[cur] = as[argmin][ks[argmin]];
            y[cur] = ys[argmin][ks[argmin]];
            ks[argmin]++;
            cur++;
        }
    }

    public static void merge(int[][] as, double[][] ys, int[] a, double[] y) {
        int[] ks = new int[as.length];
        int cur = 0;
        while (cur < a.length) {
            int argmin = -1;
            int min = Integer.MAX_VALUE;
            for (int i = 0; i < ks.length; i++) {
                if (ks[i] < as[i].length && as[i][ks[i]] < min)
                    argmin = i;
            }
            a[cur] = as[argmin][ks[argmin]];
            y[cur] = ys[argmin][ks[argmin]];
            ks[argmin]++;
            cur++;
        }
    }
}
