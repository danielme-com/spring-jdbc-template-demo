package com.danielme.spring.jdbctemplate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
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

    private static final Logger logger = LogManager.getLogger(CountryDao.class);

    private final JdbcTemplate jdbcTemplate;

    public CountryDao(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<Country> findAllPureJdbc() throws SQLException {
        try (Connection connection = jdbcTemplate.getDataSource().getConnection();
             PreparedStatement ps = connection.prepareStatement("SELECT * FROM countries");) {
            ResultSet resultSet = ps.executeQuery();
            return mapResults(resultSet);
        } catch (SQLException ex) {
            logger.error(ex);
            throw ex;
        }
    }

    private List<Country> mapResults(ResultSet resultSet) throws SQLException {
        List<Country> results = new ArrayList<>();
        while (resultSet.next()) {
            Country country = mapResult(resultSet);
            results.add(country);
        }
        return results;
    }

    private Country mapResult(ResultSet resultSet) throws SQLException {
        return new Country(
                resultSet.getLong("id"),
                resultSet.getString("name"),
                resultSet.getInt("population"));
    }

    public List<Country> findAll() {
        return jdbcTemplate.query("SELECT * FROM countries", new CountryRowMapper());
    }

    public List<Country> findByName(String name) {
        return jdbcTemplate.query("SELECT * FROM countries WHERE name LIKE ?",
                new CountryRowMapper(), name);
    }

    public Country findById(Long id) {
        return jdbcTemplate.queryForObject("SELECT * FROM countries WHERE id = ?",
                new CountryRowMapper(), id);
    }

    public int count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM countries", Integer.class);
    }

    public int deleteAll() {
        return jdbcTemplate.update("DELETE from countries");
    }

    public void insertWithQuery(String name, int population) {
        jdbcTemplate.update("INSERT INTO countries (name, population) VALUES(?,?)", name, population);
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

    public long insert(String name, int population) {
        SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(jdbcTemplate.getDataSource())
                .withTableName("countries").usingGeneratedKeyColumns("id");

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("name", name);
        parameters.put("population", population);
        return simpleJdbcInsert.executeAndReturnKey(parameters).longValue();
    }

    public Integer callProcedure(String name) {
        SimpleJdbcCall simpleJdbcCall = new SimpleJdbcCall(jdbcTemplate)
                .withProcedureName("search");

        SqlParameterSource in = new MapSqlParameterSource().addValue("name", name);

        Map<String, Object> out = simpleJdbcCall.execute(in);
        return (Integer) out.get("total");
    }

    public Integer callFunction(String name) {
        SimpleJdbcCall simpleJdbcCall = new SimpleJdbcCall(jdbcTemplate)
                .withFunctionName("search2");

        SqlParameterSource in = new MapSqlParameterSource().addValue("name", name);

        return simpleJdbcCall.executeFunction(Integer.class, in);
    }

    private static class CountryRowMapper implements RowMapper<Country> {
        @Override
        public Country mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new Country(rs.getLong("id"), rs.getString("name"), rs.getInt("population"));
        }
    }

}
