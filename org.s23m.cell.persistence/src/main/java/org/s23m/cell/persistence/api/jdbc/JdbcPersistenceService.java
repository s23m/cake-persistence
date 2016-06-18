package org.s23m.cell.persistence.api.jdbc;

import org.s23m.cell.Set;
import org.s23m.cell.api.Query;
import org.s23m.cell.api.models.S23MSemanticDomains;
import org.s23m.cell.api.models.SemanticDomain;
import org.s23m.cell.persistence.api.PersistenceService;
import org.s23m.cell.persistence.dao.IdentityDao;
import org.s23m.cell.persistence.model.Identity;
import org.s23m.cell.platform.api.Instantiation;

/**
 * Basic {@link PersistenceService} implementation
 */
public class JdbcPersistenceService implements PersistenceService {

	private final IdentityDao identityDao;

	public JdbcPersistenceService(final IdentityDao identityDao) {
		this.identityDao = identityDao;
	}

	public void store(final Set graph) {

		// for each set in sequence (iterate through in order):
		//    persist all identities first
		//    persist graph itself:
		//       either persist either edge or arrow (and if edge, persist corresponding arrow)

		// Note: http://blog.ploeh.dk/2014/08/11/cqs-versus-server-generated-ids/

		for (final Set containedInstance : graph.filterInstances()) {
			storeModel(containedInstance);
		}

		final Set containedSemanticDomains = Instantiation.toSemanticDomain(graph).filterPolymorphic(SemanticDomain.semanticdomain);
		for (final Set semanticDomain : containedSemanticDomains) {
			storeSemanticDomain(semanticDomain);
		}

	}

	private void storeModel(final Set containedInstance) {
		System.out.println("Storing model: " + containedInstance);

	}

	private void storeSemanticDomain(final Set semanticDomain) {
		System.out.println("Storing semanticDomain: " + semanticDomain);

		// serialise identities
		final Set orderedSetOfSemanticIdentities = semanticDomain.filterPolymorphic(SemanticDomain.semanticIdentity);
		for (final Set semanticIdentitySet: orderedSetOfSemanticIdentities) {
			// store the identity
			System.out.println("Storing identity: " + semanticIdentitySet.identity());
			identityDao.insert(new Identity(semanticIdentitySet.identity()));
		}

		storeStructure(semanticDomain);

		//parent.addSemanticDomain(semanticDomain);
	}

	private void storeStructure(final Set structure) {

		// process Vertex list
		for (final Set vertexInstance : structure.filterProperClass(Query.vertex)) {
			System.out.println("Storing vertex: " + vertexInstance);
			//final Vertex vertex = builder.vertex(vertexInstance);
			//container.addVertex(vertex);
		}

		// process Edge list
		for (final Set edgeInstance : structure.filterProperClass(Query.edge)) {
			System.out.println("Storing edge: " + edgeInstance);
			//final Edge edge = builder.edge(edgeInstance);
			//container.addEdge(edge);
		}

		// process Visibility list
		for (final Set visibilityInstance : structure.filterProperClass(Query.visibility)) {
			System.out.println("Storing visibility: " + visibilityInstance);
			//final Visibility visibility = builder.visibility(visibilityInstance);
			//container.addVisibility(visibility);
		}

		// process SuperSetReference list
		for (final Set superSetReferenceInstance : structure.filterProperClass(Query.superSetReference)) {
			System.out.println("Storing superSetReference: " + superSetReferenceInstance);
			//final SuperSetReference superSetReference = builder.superSetReference(superSetReferenceInstance);
			//container.addSuperSetReference(superSetReference);
		}

		// process Command list
		for (final Set commandInstance : structure.filter(S23MSemanticDomains.command)) {
			System.out.println("Storing command: " + commandInstance);
			//final Command command = builder.command(commandInstance);
			//container.addCommand(command);
		}

		// process Query list
		for (final Set queryInstance : structure.filter(S23MSemanticDomains.query)) {
			System.out.println("Storing query: " + queryInstance);

			//final org.s23m.cell.communication.xml.model.schemainstance.Query query = builder.query(queryInstance);
			//container.addQuery(query);
		}
	}
}
