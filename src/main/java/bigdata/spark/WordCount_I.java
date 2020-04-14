package bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by ua28 on 4/9/20.
 */
public class WordCount_I {

    public static void main(String[] args) {
//        SparkConf conf = new SparkConf()
//                .setAppName("WordCount_I")
//                .setMaster("local[*]");
//
//        JavaSparkContext jsc = new JavaSparkContext(conf);
//
//        // input files
//        JavaRDD<String> lines = jsc.textFile("resource/testwords/*");
//
//        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String line) throws Exception {
//                return Arrays.asList(line.split(" ")).iterator();
//            }
//        });
//
//        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(new PairFunction<String, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String word) throws Exception {
//                return new Tuple2<>(word, 1);
//            }
//        });
//
//        JavaPairRDD<String, Integer> reducedWord = wordAndOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer integer, Integer integer2) throws Exception {
//                return integer + integer2;
//            }
//        });
//
//        // output directory
//        reducedWord.saveAsTextFile("sparkwordcount_result");
//
//        jsc.stop();

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("WordCount_I");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> lines = jsc.textFile("resource");

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> reduced = wordAndOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        reduced.saveAsTextFile("result");

        jsc.stop();
    }

}
