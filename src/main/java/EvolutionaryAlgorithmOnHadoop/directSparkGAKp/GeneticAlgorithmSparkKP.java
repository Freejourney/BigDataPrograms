package EvolutionaryAlgorithmOnHadoop.directSparkGAKp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Created by ua28 on 4/17/20.
 */
public class GeneticAlgorithmSparkKP {

    private static int iteration = 100;
    private static int swarmSize = 40;
    private static int particalSize = 10;
    private static double boundMin = -1, boundMax = 1;

    private static int c1 = 2, c2 = 2;


    private static void initialParticals() {

        double[] fitnesses = new double[swarmSize];
        List<double[]> positions = new ArrayList<>();
        List<double[]> velocities = new ArrayList<>();

        StringBuffer sb = new StringBuffer();

        for (int indi = 0; indi < swarmSize; indi++) {

            double[] position = new double[particalSize];
            double[] velocity = new double[particalSize];

            Arrays.fill(position, 0);

            for (int i = 0; i < position.length; i++) {
                position[i] = new Random().nextDouble() * 2 - 1;
                velocity[i] = new Random().nextDouble() * 2 - 1;
            }

            // fitness
            double fitness = 0;
            for (int i = 0; i < position.length; i++) {
                fitness += position[i];
            }

            double[] pbest = position;
            double pbestfit = fitness;

            fitnesses[indi] = fitness;
            positions.add(position);
            velocities.add(velocity);

            String particleInfo = convert2Info(fitness, position, velocity);

            sb.append(particleInfo);
        }

        try {
            FileWriter fileWriter = new FileWriter("psoresult");
            fileWriter.write(sb.toString());
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static String convert2Info(double fitness, double[] position, double[] velocity) {
        String result = ""+fitness+"|";
        for (int i = 0; i < position.length-1; i++) {
            result+=String.valueOf(position[i])+",";
        }
        result+=String.valueOf(position[position.length-1])+"|";
        for (int i = 0; i < position.length-1; i++) {
            result+=String.valueOf(velocity[i])+",";
        }
        result+=String.valueOf(velocity[position.length-1])+"\n";

        return result;
    }



    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        JavaSparkContext jsc = new JavaSparkContext(conf);

        initialParticals();

        for (int i = 0; i < iteration; i++) {

//            readParticals();
//            findPBest();
//            findGBest();
//            updateVP();


            JavaRDD<String> input = jsc.textFile("file");

            input.mapToPair(new PairFunction<String, Object, Object>() {
                @Override
                public Tuple2<Object, Object> call(String s) throws Exception {
                    return null;
                }
            });

        }
    }
}
