package com.danielme.spring.jdbctemplate;

import static org.junit.Assert.*;

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
@Sql("reset.sql")
public class CountryDaoTest {

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
        countryDao.insertWithQuery("test", 123456);
        assertEquals(4, countryDao.findAll().size());
    }

    @Test
    public void testInsert() {
        assertEquals(countryDao.insert("test", 123456), (long) countryDao.findByName("test").get(0)
                .getId());
    }

    @Test
    public void testFind() {
        List<Country> list = countryDao.findByName("Spain");
        assertEquals(1, list.size());
        assertEquals("Spain", list.get(0).getName());
    }
    
    @Test
    public void testFindById() {
    	List<Country> list = countryDao.findByName("Spain");    	
        assertEquals("Spain", countryDao.findById(list.get(0).getId()).getName());
    }

    @Test
    public void testProcedure() {
        assertEquals(0, (int) countryDao.callProcedure("test"));
    }
    
    @Test
    public void testFunction() {
        assertEquals(0, (int) countryDao.callFunction("test"));
    }

    @Test
    public void count() {
        assertEquals(3, countryDao.count());
    }

}