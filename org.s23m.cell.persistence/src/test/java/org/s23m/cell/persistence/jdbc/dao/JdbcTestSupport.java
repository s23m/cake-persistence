package org.s23m.cell.persistence.jdbc.dao;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

import javax.sql.DataSource;

import org.apache.commons.dbutils.QueryRunner;
import org.s23m.cell.persistence.dao.jdbc.JdbcAgentDao;
import org.s23m.cell.persistence.dao.jdbc.JdbcArrowDao;
import org.s23m.cell.persistence.dao.jdbc.JdbcEdgeDao;
import org.s23m.cell.persistence.dao.jdbc.JdbcGraphDao;
import org.s23m.cell.persistence.dao.jdbc.JdbcIdentityDao;
import org.s23m.cell.persistence.model.Agent;
import org.s23m.cell.persistence.model.Arrow;
import org.s23m.cell.persistence.model.Edge;
import org.s23m.cell.persistence.model.Graph;
import org.s23m.cell.persistence.model.Identity;

import com.zaxxer.hikari.HikariDataSource;

public class JdbcTestSupport {

	static {
		// adjust SimpleLogger logging level for HikariCP - see http://www.slf4j.org/api/org/slf4j/impl/SimpleLogger.html
		System.setProperty("org.slf4j.simpleLogger.log.com.zaxxer.hikari", "error");
	}

	private HikariDataSource dataSource;

	private QueryRunner queryRunner;

	private JdbcGraphDao graphDao;

	private JdbcIdentityDao identityDao;

	private JdbcArrowDao arrowDao;

	private JdbcEdgeDao edgeDao;

	private JdbcAgentDao agentDao;

	public void initialiseTestDatabase(final String databaseName) throws SQLException {
		dataSource = JdbcTestSupport.createDatasource(databaseName);

		// clean up any previous state
		dropTables();

		executeDDL(dataSource, "sql/common_ddl.sql");
		executeDDL(dataSource, "sql/h2_ddl.sql");

		queryRunner = new QueryRunner(dataSource);

		graphDao = new JdbcGraphDao(queryRunner);
		identityDao = new JdbcIdentityDao(queryRunner);
		arrowDao = new JdbcArrowDao(queryRunner);
		edgeDao = new JdbcEdgeDao(queryRunner);
		agentDao = new JdbcAgentDao(queryRunner);
	}

	public void destroyTestDatabase() throws SQLException {
		dropTables();
		dataSource.close();
	}

	public JdbcGraphDao getGraphDao() {
		return graphDao;
	}

	public JdbcIdentityDao getIdentityDao() {
		return identityDao;
	}

	public JdbcArrowDao getArrowDao() {
		return arrowDao;
	}

	public JdbcEdgeDao getEdgeDao() {
		return edgeDao;
	}

	public JdbcAgentDao getAgentDao() {
		return agentDao;
	}

	public Connection getConnection() throws SQLException {
		return dataSource.getConnection();
	}

	private void dropTables() throws SQLException {
		// must drop tables in dependency order
		final Class<?>[] entityClasses = new Class<?>[] {
			Edge.class,
			Arrow.class,
			Graph.class,
			Agent.class,
			Identity.class
		};

		for (final Class<?> c : entityClasses) {
			dropTable(dataSource, c);
		}
	}

	private void dropTable(final DataSource dataSource, final Class<?> c) throws SQLException {
		final String sql = "DROP TABLE IF EXISTS " + c.getSimpleName();
		executeSql(dataSource, sql);
	}

	/**
	 * Creates a Data
	 * @param databaseName typically based on the test class name, so that we can run them in parallel without affecting other tests
	 * @return
	 */
	private static HikariDataSource createDatasource(final String databaseName) {
		final HikariDataSource dataSource = new HikariDataSource();
		dataSource.setDriverClassName("org.h2.Driver");

		final String tempDir = System.getProperty("java.io.tmpdir");
		final String databaseDir = tempDir + "/" + databaseName;

		final String hsqldbDriverAndProtocol = "jdbc:h2:";
		final String hsqldbUrl = hsqldbDriverAndProtocol + databaseDir;
		dataSource.setJdbcUrl(hsqldbUrl);

		dataSource.setUsername("sa");
		dataSource.setPassword("");
		return dataSource;
	}

	private void executeDDL(final DataSource dataSource, final String resourceLocation) throws SQLException {
		final InputStream stream = JdbcArrowDaoTest.class.getClassLoader().getResourceAsStream(resourceLocation);
		final String ddl = readStream(stream);
		executeSql(dataSource, ddl);
	}

	private void executeSql(final DataSource dataSource, final String sql) throws SQLException {
		final Connection connection = dataSource.getConnection();
		final Statement statement = connection.createStatement();
		statement.execute(sql);
		connection.close();
	}

	private String readStream(final InputStream is) {
		final Scanner scanner = new Scanner(is);
		final Scanner s = scanner.useDelimiter("\\A");
		try {
			return s.hasNext() ? s.next() : "";
		} finally {
			scanner.close();
		}
	}
}
