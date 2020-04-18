package EvolutionaryAlgorithmOnHadoop.directSparkGAKp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import scala.tools.nsc.settings.Final;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by ua28 on 4/17/20.
 */
public class GeneticAlgorithmSparkKP {

    private static int iteration = 100;
    private static int swarmSize = 40;
    private static int particalSize = 10;
    private static double boundMin = -1, boundMax = 1;

    private static int c1 = 2, c2 = 2;
    private static double w = 0.5;


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
            FileWriter fileWriter = new FileWriter("-1_particals_result");
            fileWriter.write(sb.toString());
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * fitness | position | velocity
     * @param fitness
     * @param position
     * @param velocity
     * @return
     */
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
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("PSO");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        initialParticals();

        JavaRDD<Partical> particles = createParticalSFromFile(jsc, iteration-1);

//        JavaRDD<Partical> results = null;
        List<Partical> percollect = particles.collect();

        double fit = 0;
        int index = 0;
        for (int j = 0; j < percollect.size(); j++) {
            if (percollect.get(j).getFitness() > fit) {
                fit = percollect.get(j).getFitness();
                index = j;
            }
        }
        final Partical gbestPartical = percollect.get(index);

        for (int i = 0; i < iteration; i++) {

//            JavaRDD<Partical> particles = createParticalSFromFile(jsc, iteration-1);
//            List<Partical> collect = particles.collect();


            JavaRDD<Partical> volocityUpdatedParticals = particles.map(new Function<Partical, Partical>() {
                @Override
                public Partical call(Partical partical) throws Exception {
                    partical.updateVelocity(gbestPartical);
                    return partical;
                }
            });

            JavaRDD<Partical> positionUpdatedParticals = volocityUpdatedParticals.map(new Function<Partical, Partical>() {
                @Override
                public Partical call(Partical partical) throws Exception {
                    partical.updatePosition();
                    partical.calculateFitness();
                    return partical;
                }
            });

            JavaRDD<Partical> pbestAndgbestUpdatedParticals = positionUpdatedParticals.map(new Function<Partical, Partical>() {
                @Override
                public Partical call(Partical partical) throws Exception {
                    if (partical.getFitness() > partical.getPbestfitness()) {
                        partical.setPbestfitness(partical.getFitness());
                        partical.updatePbest();
                    }
                    if (partical.getPbestfitness() > gbestPartical.getFitness()) {
                        gbestPartical.setFitness(partical.getPbestfitness());
                        gbestPartical.setPosition(partical.getPosition());
                    }
                    return partical;
                }
            });

//            pbestAndgbestUpdatedParticals.cache();
//            pbestAndgbestUpdatedParticals.saveAsTextFile("pso"+i);
//            pbestAndgbestUpdatedParticals.cache();
            particles = pbestAndgbestUpdatedParticals;
//            List<Partical> percollect = particles.collect();


            List<Partical> collect = particles.collect();
            for (Partical partical : collect) {
                System.out.print(partical.getFitness());
            }
            System.out.println();

        }

        particles.saveAsTextFile("pso_final_result");

//        JavaPairRDD<Double, Partical> mapped = particles.mapToPair(new PairFunction<Partical, Double, Partical>() {
//            @Override
//            public Tuple2<Double, Partical> call(Partical partical) throws Exception {
//                return new Tuple2<Double, Partical>(partical.getFitness(), partical);
//            }
//        });
//
//        JavaPairRDD<Double, Partical> sorted = mapped.sortByKey(false);
//
//        List<Partical> values = sorted.values().take(10);
//
//        for (Partical partical : values) {
//            System.out.println("fitness : " + partical.getFitness());
//        }

        jsc.stop();
    }

    private static JavaRDD<Partical> createParticalSFromFile(JavaSparkContext jsc, int i) {
        JavaRDD<String> particalsString = jsc.textFile("-1_particals_result");

        System.out.println(particalsString.take(1));

        JavaRDD<Partical> particals = particalsString.map(new Function<String, Partical>() {
            @Override
            public Partical call(String s) throws Exception {
                String[] fitness_position_velocity = s.split("\\|");
                double fitness = Double.valueOf(fitness_position_velocity[0]);
                double[] positions = AnalyzePosition(fitness_position_velocity[1]);
                double[] velocity = AnalyzeVelocity(fitness_position_velocity[2]);

                return new Partical(fitness, positions, velocity);
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

        return particals;
    }

}
