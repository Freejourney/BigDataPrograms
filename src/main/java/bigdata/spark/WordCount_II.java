package bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SparkSession;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by ua28 on 4/14/20.
 */
public class WordCount_II {

    /**
     * flat + map + reduce + collect
     * @param args
     */
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("WordCount_II")
                .master("local[*]")
                .getOrCreate();

        String input = "resource";

        JavaRDD<String> lines = sparkSession.read().textFile(input).javaRDD();

        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(Pattern.compile("[ |,|.]").split(s)).iterator());

        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((integer, integer2) -> integer + integer2);

        List<Tuple2<String, Integer>> result = counts.collect();

        for (Tuple2<?,?> tuple : result)
            System.out.println(tuple._1() + " : " + tuple._2());

        sparkSession.stop();
    }
}
