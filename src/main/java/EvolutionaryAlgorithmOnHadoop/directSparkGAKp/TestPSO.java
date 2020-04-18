package EvolutionaryAlgorithmOnHadoop.directSparkGAKp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * Created by ua28 on 4/18/20.
 */
public class TestPSO {

    private static double[] AnalyzeVelocity(String s) {
        String[] svelocity = s.split(",");
        double[] velocity = new double[svelocity.length];
        for (int i = 0; i < velocity.length; i++) {
            velocity[i] = Double.valueOf(svelocity[i]);
        }
        return velocity;
    }

    private static double[] AnalyzePosition(String s) {
        String[] spositoin = s.split(",");
        double[] position = new double[spositoin.length];
        for (int i = 0; i < spositoin.length; i++) {
            position[i] = Double.valueOf(spositoin[i]);
        }
        return position;
    }

    public static void main(String[] args) {
        String sde = "-0.2338792193375956";
        double de = Double.valueOf(sde);

        String s = "1.7707829003801085|-0.7924049403149218,0.6501175356798037,0.5230885240209875,-0.657648227351258,0.727063344573238,0.2445732004825174,0.40586839654362494,0.45767191275539165,-0.6634271700240078,0.875880324014733|0.8973722629875711,0.3278207073943884,0.617600481525123,-0.37864678653103256,0.6631274522393682,-0.28805766276534306,0.7652328190755351,0.9908237943790075,-0.607305167755831,0.944458281901013\n";
        String[] ss = s.split("\\|");
        double fitness = Double.valueOf(ss[0]);
        double[] positions = AnalyzePosition(ss[1]);
        double[] velocity = AnalyzeVelocity(ss[2]);

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("TestPSO");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> particalsString = jsc.textFile("pso");

        System.out.println(particalsString.take(1));

        JavaPairRDD<String, Partical> stringParticalJavaPairRDD = particalsString.mapToPair(new PairFunction<String, String, Partical>() {
            @Override
            public Tuple2<String, Partical> call(String s) throws Exception {
                String[] fitness_position_velocity = s.split("\\|");
                System.out.println(s);
                double fitness = Double.valueOf(fitness_position_velocity[0]);
                double[] positions = AnalyzePosition(fitness_position_velocity[1]);
                double[] velocity = AnalyzeVelocity(fitness_position_velocity[2]);
                return new Tuple2<String, Partical>(fitness_position_velocity[0], new Partical(fitness, positions, velocity));
            }

            private double[] AnalyzeVelocity(String s) {
                String[] svelocity = s.split(",");
                double[] velocity = new double[svelocity.length];
                for (int i = 0; i < velocity.length; i++) {
                    velocity[i] = Double.valueOf(svelocity[i]);
                }
                return velocity;
            }

            private double[] AnalyzePosition(String s) {
                String[] spositoin = s.split(",");
                double[] position = new double[spositoin.length];
                for (int i = 0; i < spositoin.length; i++) {
                    position[i] = Double.valueOf(spositoin[i]);
                }
                return position;
            }
        });

//        JavaRDD<Partical> particals = particalsString.map(new Function<String, Partical>() {
//            @Override
//            public Partical call(String s) throws Exception {
//                String[] fitness_position_velocity = s.split("|");
//                double fitness = Double.valueOf(fitness_position_velocity[0]);
//                double[] positions = AnalyzePosition(fitness_position_velocity[1]);
//                double[] velocity = AnalyzeVelocity(fitness_position_velocity[2]);
//
//                return new Partical(fitness, positions, velocity);
//            }
//
//            private double[] AnalyzeVelocity(String s) {
//                String[] svelocity = s.split(",");
//                double[] velocity = new double[svelocity.length];
//                for (int i = 0; i < velocity.length; i++) {
//                    velocity[i] = Double.valueOf(svelocity[i]);
//                }
//                return velocity;
//            }
//
//            private double[] AnalyzePosition(String s) {
//                String[] spositoin = s.split(",");
//                double[] position = new double[spositoin.length];
//                for (int i = 0; i < spositoin.length; i++) {
//                    position[i] = Double.valueOf(spositoin[i]);
//                }
//                return position;
//            }
//        });

        List<Tuple2<String, Partical>> collect = stringParticalJavaPairRDD.collect();
        int a = 0;
    }
}


