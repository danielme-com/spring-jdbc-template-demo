package com.danielme.spring.jdbctemplate;

public class CountryQuery {

    private final int minPopulation;
    private final int maxPopulation;

    public CountryQuery(int minPopulation, int maxPopulation) {
        this.minPopulation = minPopulation;
        this.maxPopulation = maxPopulation;
    }

    public int getMinPopulation() {
        return minPopulation;
    }

    public int getMaxPopulation() {
        return maxPopulation;
    }

}
