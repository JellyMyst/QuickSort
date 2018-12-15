import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

public class Sorter<T extends Comparable<T>> {
    public Sorter(List<T> unsorted, String master) {
        if (unsorted == null || unsorted.size() == 0)
            throw new IllegalArgumentException("Please supply actual data.");

        this.unsorted = new ArrayList<>(unsorted);
        Collections.shuffle(this.unsorted);
        this.master = master;
    }

    private final List<T> unsorted;
    private final String master;
    private long serialTime;
    private long parallelTime;

    /**
     * Sorts the list stored in this object using a serial implementation of the QuickSort algorithm.
     * @return sorted list
     */
    public List<T> serialSort() {
        List<T> result = new ArrayList<>(unsorted);

        long start = System.nanoTime();
        serialSortInternal(result);
        serialTime = System.nanoTime() - start;

        return result;
    }

    /**
     * Sorts the list passed in using the QuickSort algorithm.
     * Implementation adapted from https://www.programcreek.com/2012/11/quicksort-array-in-java/
     * @param arr list of comparable objects
     */
    private void serialSortInternal(List<T> arr) {
        // pick the pivot
        T pivot = arr.get(0);

        // make left < pivot and right > pivot
        int i = 0, j = arr.size()-1;
        while (i <= j) {
            while (arr.get(i).compareTo(pivot) < 0)
                i++;

            while (arr.get(j).compareTo(pivot) > 0)
                j--;

            if (i <= j) {
                T temp = arr.get(i);
                arr.set(i, arr.get(j));
                arr.set(j, temp);
                i++;
                j--;
            }
        }

        // recursively sort two sub parts
        if (0 < j)
            serialSortInternal(arr.subList(0, j+1));

        if (arr.size()-1 > i)
            serialSortInternal(arr.subList(i, arr.size()));
    }

    /**
     * Sorts the list stored in this object using a parallel implementation of the QuickSort algorithm.
     * @return sorted list
     */
    public List<T> parallelSort() {
        SparkConf conf = new SparkConf().setAppName("QuickSort").setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<T> data = sc.parallelize(unsorted);

        long start = System.nanoTime();
        JavaRDD<T> result = parallelSortInternal(data);
        parallelTime = System.nanoTime() - start;

        return Objects.requireNonNull(result).collect();
    }

    /**
     * Sorts the JavaRDD passed in using the QuickSort algorithm.
     * @param arr JavaRDD of comparable objects
     * @return sorted JavaRDD
     */
    private JavaRDD<T> parallelSortInternal(JavaRDD<T> arr) {
        // pick the pivot
        final T pivot = arr.first();

        // make below < pivot and above >= pivot
        JavaPairRDD<Case, T> grouped = arr
                .mapToPair((PairFunction<T, Case, T>) t -> new Tuple2<>(t.compareTo(pivot) < 0 ? Case.BELOW :
                                                                        t.compareTo(pivot) > 0 ? Case.ABOVE :
                                                                        Case.SAME, t)).cache();
        // JavaRDD<Tuple2<Boolean, T>> grouped = arr.map((T v1) -> new Tuple2<>(v1.compareTo(pivot) < 0, v1)).cache();
        JavaRDD<T> below = grouped.filter((Function<Tuple2<Case, T>, Boolean>) v1 -> v1._1() == Case.BELOW)
                .persist(StorageLevel.MEMORY_AND_DISK())
                .map((Function<Tuple2<Case, T>, T>) Tuple2::_2);
        JavaRDD<T> same = grouped.filter((Function<Tuple2<Case, T>, Boolean>) v1 -> v1._1() == Case.SAME)
                .persist(StorageLevel.MEMORY_AND_DISK())
                .map((Function<Tuple2<Case, T>, T>) Tuple2::_2);
        JavaRDD<T> above = grouped.filter((Function<Tuple2<Case, T>, Boolean>) v1 -> v1._1() == Case.ABOVE)
                .persist(StorageLevel.MEMORY_AND_DISK())
                .map((Function<Tuple2<Case, T>, T>) Tuple2::_2);

        // recursively sort two sub parts
        if (below.count() > 1)
            below = parallelSortInternal(below);

        if (above.count() > 1)
            above = parallelSortInternal(above);

        return below.union(same).union(above);
    }

    public long getSerialTime() {
        return serialTime;
    }

    public long getParallelTime() {
        return parallelTime;
    }

    private enum Case {
        BELOW, SAME, ABOVE
    }
}
