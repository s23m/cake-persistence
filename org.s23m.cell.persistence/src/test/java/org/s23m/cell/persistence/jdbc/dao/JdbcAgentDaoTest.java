package org.s23m.cell.persistence.jdbc.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.s23m.cell.persistence.jdbc.dao.TestData.createAgent;
import static org.s23m.cell.persistence.jdbc.dao.TestData.createIdentity;

import java.sql.SQLException;
import java.util.UUID;

import org.junit.Test;
import org.s23m.cell.persistence.model.Agent;
import org.s23m.cell.persistence.model.Identity;

public class JdbcAgentDaoTest extends AbstractJdbcTest {

	@Test
	public void testInsertionAndRetrieval() throws SQLException {
		final String uuid = UUID.randomUUID().toString();

		final Identity identity = createIdentity(uuid);
		final Agent agent = createAgent(uuid);

		getIdentityDao().insert(identity);
		getAgentDao().insert(agent);

		// now retrieve the result
		final Agent retrieved = getAgentDao().get(agent.getUrr());
		assertEquals(agent, retrieved);
		assertEquals(agent.hashCode(), retrieved.hashCode());
	}

	@Test
	public void testMultipleInsertionAttemptsFail() throws SQLException {
		final String uuid = UUID.randomUUID().toString();

		final Identity identity = createIdentity(uuid);
		final Agent agent = createAgent(uuid);

		getIdentityDao().insert(identity);
		getAgentDao().insert(agent);

		try {
			getAgentDao().insert(agent);
			fail("Multiple inserts should be disallowed");
		} catch (final RuntimeException e) {
			// expected
		}
	}

	@Test
	public void testUpdate() throws SQLException {
		final String uuid = UUID.randomUUID().toString();

		final Identity identity = createIdentity(uuid);
		final Agent agent = createAgent(uuid);
		final String urr = agent.getUrr();

		getIdentityDao().insert(identity);
		getAgentDao().insert(agent);

		// now retrieve the result
		final Agent a = getAgentDao().get(urr);

		// modify name and update
		final String newName = "Bobby";
		final Agent modified = new Agent(urr, uuid, a.getEmail(), a.getPassword(), a.getMobile(), newName, a.getLastName(), a.getAlias());
		getAgentDao().update(modified);

		final Agent b = getAgentDao().get(urr);
		assertEquals(newName, b.getFirstName());
	}

	@Test(expected = RuntimeException.class)
	public void testAttemptToUpdateNonExistentAgent() throws SQLException {
		final Agent agent1 = createAgent("1");
		getAgentDao().insert(agent1);
		final Agent agent2 = createAgent("2");
		getAgentDao().update(agent2);
	}

	@Test
	public void testForeignKeyConstraintViolated() {
		final String uuid = UUID.randomUUID().toString();

		final Identity identity = createIdentity(uuid);
		final Agent a = createAgent(uuid);

		getIdentityDao().insert(identity);
		getAgentDao().insert(a);

		try {
			// violate foreign key by pointing to a non-existent identity UUID
			final Agent modified = new Agent(uuid, "non-existent", a.getEmail(), a.getPassword(), a.getMobile(), a.getFirstName(), a.getLastName(), a.getAlias());

			getAgentDao().update(modified);
			fail("Violation should have thrown an exception");
		} catch (final RuntimeException e) {
			// expected
			final Throwable cause = e.getCause();
			assertTrue(cause instanceof SQLException);
			final String message = cause.getMessage();
			final String expectedPrefix = "Referential integrity constraint violation: \"FK_AGENT_UUID";
			assertTrue(message.startsWith(expectedPrefix));
		}
	}

}
