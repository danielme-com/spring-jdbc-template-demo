package com.danielme.spring.jdbctemplate;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {AppConfiguration.class})
@Sql("/reset.sql")
public class CountryDaoTest {

    private static final String SPAIN_NAME = "Spain";
    private static final String COLOMBIA_NAME = "Colombia";
    private static final String TEST_NAME = "test";
    private static final String FUNC_PROC_NAME = "test";
    private static final Long SPAIN_ID = 2L;

    @Autowired
    private CountryDao countryDao;

    @Test
    public void testAllJdbcTemplate() {
        assertEquals(3, countryDao.findAll().size());
    }

    @Test
    public void testAllJdbc() throws SQLException {
        assertEquals(3, countryDao.findAllPureJdbc().size());
    }

    @Test
    public void testDelete() {
        assertEquals(3, countryDao.deleteAll());
    }

    @Test
    public void testInsertQuery() {
        countryDao.insertWithQuery(TEST_NAME, 123456);

        assertEquals(4, countryDao.count());
    }

    @Test
    public void testInsert() {
        long idReturned = countryDao.insertWithSimpleJdbcInsert(TEST_NAME, 123456);
        long id = countryDao.findByName(TEST_NAME).get(0).getId();
        assertEquals(idReturned, id);
    }

    @Test
    public void testFindByName() {
        List<Country> countries = countryDao.findByName(SPAIN_NAME);

        assertEquals(1, countries.size());
        assertEquals(SPAIN_NAME, countries.get(0).getName());
    }

    @Test
    public void testFindByPopulation() {
        List<Country> countries = countryDao.findByPopulation(45000000, 50000000);

        assertEquals(2, countries.size());
        assertEquals(COLOMBIA_NAME, countries.get(0).getName());
        assertEquals(SPAIN_NAME, countries.get(1).getName());
    }

    @Test
    public void testFindById() {
        Optional<Country> countryOpt = countryDao.findById(SPAIN_ID);

        assertTrue(countryOpt.isPresent());
        assertEquals(SPAIN_NAME, countryOpt.get().getName());
    }

    @Test
    public void testProcedure() {
        assertEquals(0, (int) countryDao.callProcedure(FUNC_PROC_NAME));
    }

    @Test
    public void testFunction() {
        assertEquals(0, (int) countryDao.callFunction(FUNC_PROC_NAME));
    }

    @Test
    public void testCount() {
        assertEquals(3, countryDao.count());
    }

    @Test
    public void testBatchInsert() {
        List<Country> countries = IntStream.rangeClosed(1, 500)
                .boxed()
                .map(i -> new Country(String.valueOf(i), i))
                .collect(Collectors.toList());
        long init = System.currentTimeMillis();

        countryDao.insertBatch(countries, 100);

        assertEquals(countries.size() + 3, countryDao.count());
        System.out.println(System.currentTimeMillis() - init + " ms");
    }

}