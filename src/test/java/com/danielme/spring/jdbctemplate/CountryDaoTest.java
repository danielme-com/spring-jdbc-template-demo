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
import java.util.function.Consumer;
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
    private static final int POPULATION_TEST = 123456;
    private static final Long SPAIN_ID = 2L;
    public static final int COUNTRIES_SIZE = 3;

    @Autowired
    private CountryDao countryDao;

    @Test
    public void testAllJdbcTemplate() {
        assertEquals(3, countryDao.findAll().size());
    }

    @Test
    public void testAllJdbc() throws SQLException {
        assertEquals(COUNTRIES_SIZE, countryDao.findAllPureJdbc().size());
    }

    @Test
    public void testDelete() {
        assertEquals(COUNTRIES_SIZE, countryDao.deleteAll());
    }

    @Test
    public void testInsertQuery() {
        countryDao.insertWithQuery(TEST_NAME, POPULATION_TEST);

        assertEquals(COUNTRIES_SIZE + 1, countryDao.count());
    }

    @Test
    public void testInsertAsParameters() {
        long idReturned = countryDao.insertWithSimpleJdbcInsert(TEST_NAME, POPULATION_TEST);
        long id = countryDao.findByName(TEST_NAME).get(0).getId();
        assertEquals(idReturned, id);
    }

    @Test
    public void testInsertAsObject() {
        Country country = new Country(TEST_NAME, POPULATION_TEST);
        long idReturned = countryDao.insertWithSimpleJdbcInsert(country);
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
    public void testFindByPopulationWithParams() {
        List<Country> countries = countryDao.findByPopulation(45000000, 50000000);

        assertFindByPopulation(countries);
    }

    @Test
    public void testFindByPopulationWithObject() {
        List<Country> countries = countryDao.findByPopulation(new CountryQuery(45000000, 50000000));

        assertFindByPopulation(countries);
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
        assertEquals(COUNTRIES_SIZE, countryDao.count());
    }

    @Test
    public void testBatchInsertWithSize() {
        testBatchInsert((countries) -> countryDao.insertBatch(countries, 100));
    }

    @Test
    public void testBatchInsertNoSize() {
        testBatchInsert((countries) -> countryDao.insertBatch(countries));
    }

    @Test
    public void testBatchUpdate() {
        testBatchUpdate((countries) -> countryDao.updateBatch(countries, 100));
    }

    @Test
    public void testBatchUpdateNamed() {
        testBatchUpdate((countries) -> countryDao.updateBatchNamed(countries));
    }

    private void testBatchUpdate(Consumer<List<Country>> consumer) {
        List<Country> countries = countryDao.findAll();
        countries.forEach(c -> c.setPopulation(0));

        consumer.accept(countries);

        countryDao.findAll()
                .forEach(c -> assertEquals(0, c.getPopulation().longValue()));
    }

    private void testBatchInsert(Consumer<List<Country>> consumer) {
        List<Country> countries = IntStream.rangeClosed(1, 500)
                .boxed()
                .map(i -> new Country(String.valueOf(i), i))
                .collect(Collectors.toList());
        long init = System.currentTimeMillis();

        consumer.accept(countries);

        assertEquals(countries.size() + COUNTRIES_SIZE, countryDao.count());
        System.out.println(System.currentTimeMillis() - init + " ms");
    }

    private void assertFindByPopulation(List<Country> countries) {
        assertEquals(2, countries.size());
        assertEquals(COLOMBIA_NAME, countries.get(0).getName());
        assertEquals(SPAIN_NAME, countries.get(1).getName());
    }


}