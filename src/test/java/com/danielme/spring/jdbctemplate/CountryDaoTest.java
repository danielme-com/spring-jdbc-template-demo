package com.danielme.spring.jdbctemplate;

import static org.junit.Assert.*;

import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.danielme.spring.jdbctemplate.ApplicationContext;
import com.danielme.spring.jdbctemplate.CountryDao;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { ApplicationContext.class })
@Sql("/reset.sql")
public class CountryDaoTest {

    private static final String SPAIN_NAME = "Spain";
    private static final String TEST_NAME = "test";
    private static final String FUNC_PROC_NAME = "test";
    private static final Long SPAIN_ID = 2L;

    @Autowired
    private CountryDao countryDao;

    @Test
    public void testAll() {
        assertEquals(3, countryDao.findAll().size());
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
        assertEquals(countryDao.insert(TEST_NAME, 123456),
                (long) countryDao.findByName(TEST_NAME).get(0).getId());
    }

    @Test
    public void testFindByName() {
        List<Country> list = countryDao.findByName(SPAIN_NAME);
        assertEquals(1, list.size());
        assertEquals(SPAIN_NAME, list.get(0).getName());
    }

    @Test
    public void testFindById() {
        Country country = countryDao.findById(SPAIN_ID);
        assertNotNull(country);
        assertEquals(SPAIN_NAME, country.getName());
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
        int quantity = 500;
        List<Country> countries = buildCountriesList(quantity);
        long init = System.currentTimeMillis();
        countryDao.insertBatch(countries, 100);
        assertEquals(quantity + 3, countryDao.count());
        System.out.println(System.currentTimeMillis() - init + " ms");
    }

    private List<Country> buildCountriesList(int quantity) {
        List<Country> countries = new LinkedList<Country>();
        for (int i = 0; i < quantity; i++) {
            countries.add(new Country(String.valueOf(i), i));
        }
        return countries;
    }

}