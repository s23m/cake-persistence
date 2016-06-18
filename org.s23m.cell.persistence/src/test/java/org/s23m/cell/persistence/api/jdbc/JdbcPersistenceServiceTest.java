package org.s23m.cell.persistence.api.jdbc;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.s23m.cell.Set;
import org.s23m.cell.persistence.dao.jdbc.JdbcIdentityDao;
import org.s23m.cell.persistence.jdbc.dao.JdbcTestSupport;
import org.s23m.cell.persistence.model.Identity;
import org.s23m.cell.platform.api.AgencyTestFoundationTestCase;
import org.s23m.cell.platform.testfoundation.AgencyTestFoundation;

public class JdbcPersistenceServiceTest extends AgencyTestFoundationTestCase {

	private final List<Set> exampleModels = new ArrayList<Set>();

	private final JdbcTestSupport support;

	public JdbcPersistenceServiceTest() {
		this.support = new JdbcTestSupport();
	}

	@Override
	protected void doAdditionalSetup() {
		// only instances of Agents or any of their contained models ever need to be serialised
		// ithanku is an example of an instance of an Agent.
		exampleModels.add(AgencyTestFoundation.ithanku);
		exampleModels.add(AgencyTestFoundation.ernst);

		try {
			support.initialiseTestDatabase(getClass().getSimpleName());
		} catch (final SQLException e) {
			throw new IllegalStateException("Could not initialise test database", e);
		}
	}

	public void testStoreIThankUInstance() throws SQLException {
		final JdbcIdentityDao identityDao = support.getIdentityDao();
		final JdbcPersistenceService service = new JdbcPersistenceService(identityDao);

		service.store(AgencyTestFoundation.ithanku);
		final Long identityCount = executeCountQuery("select count(*) from Identity");
		assertThat(identityCount, is(15L));

		final Identity giver = identityDao.get("32779ea2-89cf-11df-a4ee-0800200c9a67");
		assertThat(giver.getName(), is("giver"));
	}

	public void testStoreErnstInstance() throws SQLException {
		final JdbcIdentityDao identityDao = support.getIdentityDao();
		final JdbcPersistenceService service = new JdbcPersistenceService(identityDao);

		service.store(AgencyTestFoundation.ernst);
		final Long identityCount = executeCountQuery("select count(*) from Identity");
		assertThat(identityCount, is(5L));

		final Identity giver = identityDao.get("3277c5b8-89cf-11df-a4ee-0800200c9a67");
		assertThat(giver.getName(), is("ErnstNativeLanguage"));
	}

	private Long executeCountQuery(final String sql) throws SQLException {
		final Connection connection = support.getConnection();
		final Statement statement = connection.createStatement();
		final ResultSet resultSet = statement.executeQuery(sql);
		resultSet.next();
		final Long result = resultSet.getLong(1);
		connection.close();
		return result;
	}
}