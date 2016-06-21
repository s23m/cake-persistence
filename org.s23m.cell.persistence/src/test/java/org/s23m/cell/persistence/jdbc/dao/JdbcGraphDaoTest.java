package org.s23m.cell.persistence.jdbc.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.s23m.cell.persistence.jdbc.dao.TestData.createGraph;
import static org.s23m.cell.persistence.jdbc.dao.TestData.createIdentity;

import java.sql.SQLException;
import java.util.UUID;

import org.junit.Test;
import org.s23m.cell.persistence.model.Graph;
import org.s23m.cell.persistence.model.Identity;
import org.s23m.cell.persistence.model.ProperClass;

public class JdbcGraphDaoTest extends AbstractJdbcTest {

	@Test
	public void testInsertionAndRetrieval() throws SQLException {
		final String uuid = UUID.randomUUID().toString();
		final Identity identity = createIdentity(uuid);
		final Graph graph = createGraph(uuid, ProperClass.Vertex);

		getIdentityDao().insert(identity);
		getGraphDao().insert(graph);

		final Graph retrieved = getGraphDao().get(graph.getUrr());
		assertEquals(graph, retrieved);
		assertEquals(graph.hashCode(), retrieved.hashCode());
	}

	@Test
	public void testMultipleInsertionAttemptsFail() throws SQLException {
		final String uuid = UUID.randomUUID().toString();
		final Identity identity = createIdentity(uuid);
		final Graph graph = createGraph(uuid, ProperClass.Vertex);

		getIdentityDao().insert(identity);
		getGraphDao().insert(graph);

		try {
			getGraphDao().insert(graph);
			fail("Multiple inserts should be disallowed");
		} catch (final RuntimeException e) {
			// expected
		}
	}

	@Test
	public void testUpdate() throws SQLException {
		final String uuid = "1";

		final Identity identity = createIdentity(uuid);
		final Graph graph = createGraph(uuid, ProperClass.Vertex);

		getIdentityDao().insert(identity);
		getGraphDao().insert(graph);

		// retrieve the result
		final Graph retrieved1 = getGraphDao().get(uuid);
		assertEquals(uuid, retrieved1.getContainer());

		// store another identity
		final String uuid2 = "2";
		final Identity identity2 = createIdentity(uuid2);
		getIdentityDao().insert(identity2);

		// modify property and update
		final Graph modified = new Graph(graph.getUrr(), graph.getUuid(), graph.getCategory(), uuid2, graph.getIsAbstractValue(), graph.getProperClass(), graph.getMaxCardinalityValueInContainer(), graph.getContentAsXml());
		getGraphDao().update(modified);

		final Graph retrieved2 = getGraphDao().get(uuid);
		assertEquals(uuid2, retrieved2.getContainer());
	}

	@Test(expected = RuntimeException.class)
	public void testAttemptToUpdateNonExistentGraph() throws SQLException {
		final Graph graph1 = createGraph("1", ProperClass.Vertex);
		getGraphDao().insert(graph1);
		final Graph graph2 = createGraph("2", ProperClass.Vertex);
		getGraphDao().update(graph2);
	}

	@Test
	public void testForeignKeyConstraintViolated() {
		final String uuid = "1";

		final Identity identity = createIdentity(uuid);
		final Graph graph = createGraph(uuid, ProperClass.Vertex);

		getIdentityDao().insert(identity);
		getGraphDao().insert(graph);

		try {
			// violate foreign key by pointing to a non-existent identity UUID
			final Graph modified = new Graph(graph.getUrr(), graph.getUuid(), "2", graph.getContainer(), graph.getIsAbstractValue(), graph.getProperClass(), graph.getMaxCardinalityValueInContainer(), graph.getContentAsXml());
			getGraphDao().update(modified);
			fail("Violation should have thrown an exception");
		} catch (final RuntimeException e) {
			// expected
			final Throwable cause = e.getCause();
			assertTrue(cause instanceof SQLException);
			final String message = cause.getMessage();
			final String expectedPrefix = "Referential integrity constraint violation: \"FK_CATEGORY";
			assertTrue(message.startsWith(expectedPrefix));
		}
	}
}
