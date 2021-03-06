package org.s23m.cell.persistence.jdbc.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.s23m.cell.persistence.jdbc.dao.TestData.createArrow;
import static org.s23m.cell.persistence.jdbc.dao.TestData.createEdge;
import static org.s23m.cell.persistence.jdbc.dao.TestData.createGraph;
import static org.s23m.cell.persistence.jdbc.dao.TestData.createIdentity;

import java.sql.SQLException;
import java.util.UUID;

import org.junit.Test;
import org.s23m.cell.persistence.model.Arrow;
import org.s23m.cell.persistence.model.Edge;
import org.s23m.cell.persistence.model.Graph;
import org.s23m.cell.persistence.model.Identity;
import org.s23m.cell.persistence.model.ProperClass;

public class JdbcEdgeDaoTest extends AbstractJdbcTest {

	@Test
	public void testInsertionAndRetrieval() throws SQLException {
		final String uuid = "1";
		final Identity identity = createIdentity(uuid);
		final Graph graph = createGraph(uuid, ProperClass.Vertex);
		final Arrow arrow = createArrow(uuid, ProperClass.Visibility);
		final Edge edge = createEdge(uuid);

		getIdentityDao().insert(identity);
		getGraphDao().insert(graph);
		getArrowDao().insert(arrow);
		getEdgeDao().insert(edge);

		// now retrieve the result
		final Edge retrieved = getEdgeDao().get(edge.getUrr());
		assertEquals(edge, retrieved);
		assertEquals(edge.hashCode(), retrieved.hashCode());
	}

	@Test
	public void testMultipleInsertionAttemptsFail() throws SQLException {
		final String uuid = UUID.randomUUID().toString();

		final Identity identity = createIdentity(uuid);
		final Graph graph = createGraph(uuid, ProperClass.Vertex);
		final Arrow arrow = createArrow(uuid, ProperClass.Visibility);
		final Edge edge = createEdge(uuid);

		getIdentityDao().insert(identity);
		getGraphDao().insert(graph);
		getArrowDao().insert(arrow);
		getEdgeDao().insert(edge);

		try {
			getEdgeDao().insert(edge);
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
		final Arrow arrow = createArrow(uuid, ProperClass.Visibility);
		final Edge edge = createEdge(uuid);

		getIdentityDao().insert(identity);
		getGraphDao().insert(graph);
		getArrowDao().insert(arrow);
		getEdgeDao().insert(edge);

		// now retrieve the result
		final Edge retrieved1 = getEdgeDao().get(uuid);
		assertEquals(uuid, retrieved1.getIsNavigableValueToEdgeEnd());

		// store another identity
		final String uuid2 = "2";
		final Identity identity2 = createIdentity(uuid2);
		getIdentityDao().insert(identity2);

		// modify edge property and update
		final Edge modified = new Edge(edge.getUrr(), edge.getMinCardinalityValueFromEdgeEnd(), edge.getMinCardinalityValueToEdgeEnd(),
				edge.getMaxCardinalityValueFromEdgeEnd(), edge.getMaxCardinalityValueToEdgeEnd(),
				edge.getIsNavigableValueFromEdgeEnd(), uuid2, edge.getIsContainerValueFromEdgeEnd(),
				edge.getIsContainerValueToEdgeEnd(), edge.getFromEdgeEnd(), edge.getToEdgeEnd());

		getEdgeDao().update(modified);

		final Edge retrieved2 = getEdgeDao().get(uuid);
		assertEquals(uuid2, retrieved2.getIsNavigableValueToEdgeEnd());
	}

	@Test(expected = RuntimeException.class)
	public void testAttemptToUpdateNonExistentEdge() throws SQLException {
		final Edge edge1 = createEdge("1");
		getEdgeDao().insert(edge1);
		final Edge edge2 = createEdge("2");
		getEdgeDao().update(edge2);
	}

	@Test
	public void testForeignKeyConstraintViolated() {
		final String uuid = "1";

		final Identity identity = createIdentity(uuid);
		final Graph graph = createGraph(uuid, ProperClass.Vertex);
		final Arrow arrow = createArrow(uuid, ProperClass.Visibility);
		final Edge edge = createEdge(uuid);

		getIdentityDao().insert(identity);
		getGraphDao().insert(graph);
		getArrowDao().insert(arrow);
		getEdgeDao().insert(edge);

		try {
			// violate foreign key by pointing to a non-existent identity UUID
			final Edge modified = new Edge(edge.getUrr(), edge.getMinCardinalityValueFromEdgeEnd(), edge.getMinCardinalityValueToEdgeEnd(),
					edge.getMaxCardinalityValueFromEdgeEnd(), edge.getMaxCardinalityValueToEdgeEnd(),
					edge.getIsNavigableValueFromEdgeEnd(), edge.getIsNavigableValueToEdgeEnd(), "2",
					edge.getIsContainerValueToEdgeEnd(), edge.getFromEdgeEnd(), edge.getToEdgeEnd());

			getEdgeDao().update(modified);
			fail("Violation should have thrown an exception");
		} catch (final RuntimeException e) {
			// expected
			final Throwable cause = e.getCause();
			assertTrue(cause instanceof SQLException);
			final String message = cause.getMessage();
			final String expectedPrefix = "Referential integrity constraint violation: \"FK_ISCONTAINERVALUEFROMEDGEEND";
			assertTrue(message.startsWith(expectedPrefix));
		}
	}
}
