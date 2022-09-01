package com.danielme.spring.jdbctemplate;

public class CountryQuery {

    private int minPopulation;
    private int maxPopulation;

    public CountryQuery(int minPopulation, int maxPopulation) {
        this.minPopulation = minPopulation;
        this.maxPopulation = maxPopulation;
    }

    public int getMinPopulation() {
        return minPopulation;
    }

    public void setMinPopulation(int minPopulation) {
        this.minPopulation = minPopulation;
    }

    public int getMaxPopulation() {
        return maxPopulation;
    }

    public void setMaxPopulation(int maxPopulation) {
        this.maxPopulation = maxPopulation;
    }
}
