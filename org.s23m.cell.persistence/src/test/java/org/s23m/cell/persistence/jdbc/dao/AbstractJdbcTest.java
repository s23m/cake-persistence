package org.s23m.cell.persistence.jdbc.dao;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.s23m.cell.persistence.dao.jdbc.JdbcAgentDao;
import org.s23m.cell.persistence.dao.jdbc.JdbcArrowDao;
import org.s23m.cell.persistence.dao.jdbc.JdbcEdgeDao;
import org.s23m.cell.persistence.dao.jdbc.JdbcGraphDao;
import org.s23m.cell.persistence.dao.jdbc.JdbcIdentityDao;

public abstract class AbstractJdbcTest {

	private final JdbcTestSupport support;

	public AbstractJdbcTest() {
		this.support = new JdbcTestSupport();
	}

	@Before
	public void setUp() throws Exception {
		support.initialiseTestDatabase(getClass().getSimpleName());
	}

	@After
	public void tearDown() throws SQLException {
		support.destroyTestDatabase();
	}

	protected JdbcGraphDao getGraphDao() {
		return support.getGraphDao();
	}

	protected JdbcIdentityDao getIdentityDao() {
		return support.getIdentityDao();
	}

	protected JdbcArrowDao getArrowDao() {
		return support.getArrowDao();
	}

	protected JdbcEdgeDao getEdgeDao() {
		return support.getEdgeDao();
	}

	protected JdbcAgentDao getAgentDao() {
		return support.getAgentDao();
	}

	protected Connection getConnection() throws SQLException {
		return support.getConnection();
	}
}
