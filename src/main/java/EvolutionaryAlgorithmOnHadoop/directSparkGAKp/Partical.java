package EvolutionaryAlgorithmOnHadoop.directSparkGAKp;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import scala.reflect.ClassTag;

import java.io.Serializable;
import java.util.Random;

/**
 * Created by ua28 on 4/18/20.
 */
public class Partical implements Serializable {

    private static final long serialVersionUID = -160767830542290296L;

    private double[] velocity;
    private double[] position;
    private double fitness;
    private double[] pbest;
    private double pbestfitness;

    private double[] gbest;
    private double gbestfitness;

    private int c1 = 2, c2 = 2;
    private double w = 0.5;

    public Partical(double fitness, double[] positions, double[] velocity) {
        super();
        this.fitness = fitness;
        for (int i = 0; i < positions.length; i++) {
            this.position[i] = positions[i];
            this.velocity[i] = velocity[i];
        }
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public double[] getVelocity() {
        return velocity;
    }

    public void setVelocity(double[] velocity) {
        this.velocity = velocity;
    }

    public double[] getPosition() {
        return position;
    }

    public void setPosition(double[] position) {
        for (int i = 0; i < position.length; i++) {
            this.position[i] = position[i];
        }
    }

    public double getFitness() {
        return fitness;
    }

    public void setFitness(double fitness) {
        this.fitness = fitness;
    }

    public double[] getPbest() {
        return pbest;
    }

    public void setPbest(double[] pbest) {
        this.pbest = pbest;
    }

    public double getPbestfitness() {
        return pbestfitness;
    }

    public void setPbestfitness(double pbestfitness) {
        this.pbestfitness = pbestfitness;
    }

    public double[] getGbest() {
        return gbest;
    }

    public void setGbest(double[] gbest) {
        this.gbest = gbest;
    }

    public double getGbestfitness() {
        return gbestfitness;
    }

    public void setGbestfitness(double gbestfitness) {
        this.gbestfitness = gbestfitness;
    }

    public void updateVelocity() {
        for (int i = 0; i < this.velocity.length; i++) {
            this.velocity[i] = w*this.velocity[i] + c1*new Random().nextDouble()*(pbest[i]-position[i])
                    + c2*new Random().nextDouble()*(gbest[i]-position[i]);
        }
    }

    public void updatePosition() {
        for (int i = 0; i < velocity.length; i++) {
            position[i] += velocity[i];
        }
    }

    public double calculateFitness() {
        double fitness = 0;
        for (int i = 0; i < position.length; i++) {
            fitness += position[i];
        }
        return fitness;
    }

    public void updatePbest() {
        for (int i = 0; i < position.length; i++) {
            pbest[i] = position[i];
        }
    }
}
