package EvolutionaryAlgorithmOnHadoop.onemax.elephant56.user.common;

import it.unisa.elephant56.user.common.Individual;

import java.util.ArrayList;
import java.util.List;

public class BitStringIndividual extends Individual {
    private List<Boolean> bits;

    public BitStringIndividual(int size) {
        this.bits = new ArrayList<Boolean>(size);
    }

    public BitStringIndividual(BitStringIndividual original) {
        this.bits = new ArrayList<Boolean>(original.size());
        for (int i = 0; i < original.size(); i++)
            this.bits.set(i, original.get(i));
    }

    public void set(int index, boolean value) {
        if (index == this.bits.size()) {
            this.bits.add(index, value);
        } else {
            this.bits.set(index, value);
        }
    }

    public boolean get(int index) {
        return this.bits.get(index);
    }

    public int size() {
        return this.bits.size();
    }

    @Override
    public String toString() {
        String str = "";
        for (boolean bit : bits) {
            if (bit)
                str += "1";
            else
                str += "0";
        }
        return str;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return new BitStringIndividual(this);
    }

    @Override
    public int hashCode() {
        return this.bits != null ? this.bits.hashCode() : 0;
    }
}
