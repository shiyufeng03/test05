package com.sdw.test05.algorithm;

public class MaximumSumSubSequences {

    public static void main(String[] args) {
        int[] a = { -2, 11, -4, 13, -5, 2, -5, -3, 12, -9 };
        int sum = maxsub(a, a.length);
        System.out.println("sum = " + sum);

        int sum2 = maxsubImproved(a, a.length);
        System.out.println("max sum = " + sum2);

        int sum3 = maxsubDynamicProgramming(a, a.length);
        System.out.println("max sum = " + sum3);
    }

    public static int maxsub(final int a[], int n) {
        int sum, max, i, j, begin, end;
        begin = end = max = 0;
        for (i = 0; i < n; i++) {
            sum = 0;
            for (j = i; j < n; j++) {
                sum += a[j];
                System.out.println(String.format("the second level loop %d loop sum = %d\n", j, sum));
                System.out.println(String.format("the second level loop %d loop max = %d\n", j, max));
                if (sum > max) {
                    max = sum;
                    begin = i;
                    end = j;
                }
            }
            System.out.println(String.format("the %d loop max = %d\n", i + 1, max));
        }
        System.out.println(String.format("--final-- Begin = %d, End = %d\n", begin, end));
        return max;
    }

    // Sum(i, j+1) = Sum(i, j) + A[j+1]
    public static int maxsubImproved(int[] a, int n) {
        int max = 0;
        for (int i = 0; i < n; i++) {
            int tmp_max = 0;

            for (int j = i; j < n; j++) {
                tmp_max += a[j];
                if (tmp_max > max) {
                    max = tmp_max;
                }
            }
        }

        return max;
    }

    /**
     * 在这一遍扫描数组当中，从左到右记录当前子序列的和temp_sum，若这个和不断增加，那么最大子序列的和max也不断增加(不断更新max)。
     * 如果往前扫描中遇到负数，那么当前子序列的和将会减小。此时temp_sum
     * 将会小于max，当然max也就不更新。如果temp_sum降到0时，说明前面已经扫描的那一段就可以抛弃了
     * ，这时将temp_sum置为0。然后，temp_sum将从后面开始将这个子段进行分析
     * ，若有比当前max大的子段，继续更新max。这样一趟扫描结果也就出来了。
     * 
     * @param a
     * @param n
     * @return
     */
    public static int maxsubDynamicProgramming(int[] a, int n) {
        int max = 0;
        int tmp_max = 0;

        for (int i = 0; i < n; i++) {
            tmp_max += a[i];
            if (tmp_max > max) {
                max = tmp_max;
            }
            if (tmp_max < 0) {
                tmp_max = 0;
            }
        }

        return max;
    }

}
