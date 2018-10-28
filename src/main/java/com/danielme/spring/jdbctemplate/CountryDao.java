package com.danielme.spring.jdbctemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.simple.SimpleJdbcCall;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.stereotype.Repository;

@Repository
public class CountryDao {

    private static final Logger logger = LogManager.getLogger(CountryDao.class);

    private final JdbcTemplate jdbcTemplate;

    public CountryDao(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<Country> findAllPureJdbc() {
        List<Country> results = new LinkedList<>();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = jdbcTemplate.getDataSource().getConnection();
            preparedStatement = connection.prepareStatement("SELECT * FROM country");
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                Country country = new Country(resultSet.getLong("id"), resultSet.getString("name"),
                        resultSet.getInt("population"));

                results.add(country);
            }
        } catch (SQLException ex) {
            logger.error(ex);
            throw new RuntimeException(ex);
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException ex) {
                    logger.warn(ex);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    logger.warn(ex);
                }
            }
        }

        return results;
    }

    public List<Country> findAll() {
        return jdbcTemplate.query("SELECT * FROM country", new CountryRowMapper());
    }

    public List<Country> findByName(String name) {
        return jdbcTemplate.query("SELECT * FROM country WHERE name LIKE ?", new Object[] { name },
                new CountryRowMapper());
    }

    public Country findById(Long id) {
        return jdbcTemplate.queryForObject("SELECT * FROM country WHERE id = ?",
                new Object[] { id }, new CountryRowMapper());
    }

    public int count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM country", Integer.class);
    }

    public int deleteAll() {
        return jdbcTemplate.update("DELETE from country");
    }

    public void insertWithQuery(String name, int population) {
        jdbcTemplate.update("INSERT INTO country (name, population) VALUES(?,?)", name, population);
    }

    public void insertBatch(List<Country> countries, int batchSize) {
        String sql = "INSERT INTO country (name, population) VALUES(?,?)";

        jdbcTemplate.batchUpdate(sql, countries, batchSize,
                (PreparedStatement ps, Country country) -> {
                    ps.setString(1, country.getName());
                    ps.setInt(2, country.getPopulation());
                }
        );
    }

    public long insert(String name, int population) {
        SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(jdbcTemplate.getDataSource())
                .withTableName("country").usingGeneratedKeyColumns("id");

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
