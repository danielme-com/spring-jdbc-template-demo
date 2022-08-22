package com.danielme.spring.jdbctemplate;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.simple.SimpleJdbcCall;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

@Repository
public class CountryDao {

    private final JdbcTemplate jdbcTemplate;
    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    public CountryDao(JdbcTemplate jdbcTemplate, NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
    }

    public List<Country> findAllPureJdbc() throws SQLException {
        try (Connection connection = jdbcTemplate.getDataSource().getConnection();
             PreparedStatement ps = connection.prepareStatement("SELECT * FROM countries");) {
            ResultSet resultSet = ps.executeQuery();
            return mapResults(resultSet);
        }
    }

    private List<Country> mapResults(ResultSet resultSet) throws SQLException {
        List<Country> results = new ArrayList<>();
        while (resultSet.next()) {
            Country country = mapToCountry(resultSet);
            results.add(country);
        }
        return results;
    }

    public List<Country> findAll() {
        return jdbcTemplate.query("SELECT * FROM countries ORDER BY NAME", (rs, rowNum) -> mapToCountry(rs));
    }

    public List<Country> findByName(String name) {
        return jdbcTemplate.query("SELECT * FROM countries WHERE name LIKE ?",
                (rs, rowNum) -> mapToCountry(rs), name);
    }

    public Optional<Country> findById(Long id) {
        Country country = jdbcTemplate.queryForObject("SELECT * FROM countries WHERE id = ?",
                (rs, rowNum) -> mapToCountry(rs), id);
        return Optional.ofNullable(country);
    }

    public List<Country> findByPopulation(int minPopulation, int maxPopulation) {
        String sql = "SELECT * FROM countries WHERE population " +
                "BETWEEN :minPopulation AND :maxPopulation ORDER BY name";
        Map<String, Integer> params = new HashMap<>();
        params.put("minPopulation", minPopulation);
        params.put("maxPopulation", maxPopulation);
        return namedParameterJdbcTemplate.query(sql, params, (rs, rowNum) -> mapToCountry(rs));
    }

    public int count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM countries", Integer.class);
    }

    public int deleteAll() {
        return jdbcTemplate.update("DELETE from countries");
    }

    public void insertWithQuery(String name, int population) {
        String sql = "INSERT INTO countries (name, population) VALUES(?,?)";
        jdbcTemplate.update(sql, name, population);
    }

    public void insertBatch(List<Country> countries, int batchSize) {
        String sql = "INSERT INTO countries (name, population) VALUES(?,?)";

        jdbcTemplate.batchUpdate(sql, countries, batchSize,
                (PreparedStatement ps, Country country) -> {
                    ps.setString(1, country.getName());
                    ps.setInt(2, country.getPopulation());
                }
        );
    }

    public long insertWithSimpleJdbcInsert(String name, int population) {
        SimpleJdbcInsert simpleJdbcInsert =
                new SimpleJdbcInsert(jdbcTemplate.getDataSource())
                        .withTableName("countries")
                        .usingGeneratedKeyColumns("id");

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("name", name);
        parameters.put("population", population);
        return simpleJdbcInsert.executeAndReturnKey(parameters).longValue();
    }

    public Integer callProcedure(String name) {
        SimpleJdbcCall simpleJdbcCall = new SimpleJdbcCall(jdbcTemplate)
                .withProcedureName("proc_count_countries_by_name");
        SqlParameterSource in = new MapSqlParameterSource().addValue("name", name);
        Map<String, Object> out = simpleJdbcCall.execute(in);
        return (Integer) out.get("total");
    }

    public Integer callFunction(String name) {
        SimpleJdbcCall simpleJdbcCall = new SimpleJdbcCall(jdbcTemplate)
                .withFunctionName("func_count_countries_by_name");
        SqlParameterSource in = new MapSqlParameterSource().addValue("name", name);
        return simpleJdbcCall.executeFunction(Integer.class, in);
    }

    /*private static class CountryRowMapper implements RowMapper<Country> {
        @Override
        public Country mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new Country(rs.getLong("id"), rs.getString("name"), rs.getInt("population"));
        }
    }*/

    private Country mapToCountry(ResultSet rs) throws SQLException {
        return new Country(
                rs.getLong("id"),
                rs.getString("name"),
                rs.getInt("population"));
    }

}
