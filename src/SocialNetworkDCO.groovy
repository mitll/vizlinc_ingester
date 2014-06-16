import static edu.mit.ll.vizlincdb.util.VizLincProperties.*
import edu.mit.ll.vizlincdb.graph.VizLincDB

import com.tinkerpop.blueprints.impls.neo4j.*
import com.tinkerpop.gremlin.groovy.*
import com.tinkerpop.blueprints.*
import com.tinkerpop.gremlin.*

// Build a social network using doc co-occurrence

class SocialNetworkDCO {

    // Fields
    VizLincDB db

    SocialNetworkDCO(VizLincDB graphdb) {
        db = graphdb
    }

    // For running standalone.
    static main(args) {
        Gremlin.load()
        if (args.size() != 1) {
            println "Usage: SocialNetworkDCO.groovy <db_name>"
            System.exit(1)
        }
        def db = new VizLincDB(args[0])
        SocialNetworkDCO sn = new SocialNetworkDCO(db)
        sn.computeSocialNetwork()
        db.shutdown()
    }

    // Return the edge that exists between n1 and n2 or null if there isn't one.
    def existing_edge (n1, n2) {
        def edges = n1.bothE.as('x').bothV.retain([n2]).back('x').toList()
        switch (edges.size()) {
        case 0:
            return null
        case 1:
            // println "edge: ${n1.getProperty('entity_text')} -- ${n2.getProperty('entity_text')}"
            return edges[0]
        default:
            println "ERROR: multiple edges between $n1 and $n2"
            System.exit(2)
        }

    }


    void computeSocialNetwork() {
        // Query for docs
        def doc_nodes = db.getDocuments()
        def i = 0
        def edge_count = 0
        for (doc in doc_nodes) {
            // Find persons mentioned in a document
            def per_list = doc.outE(L_DOCUMENT_TO_MENTION).inV.outE(L_MENTION_TO_ENTITY).inV.has(P_CREATED_BY, "across_doc_person_coref").unique().toList()

            // Status
            println "doc " + i + ", id " + doc.getId() + ", Per list size " + per_list.size()

            // Connect co-occurrences: try all pairs of people.
            for (int j=0; j<per_list.size(); j++) {
                for (int k=j+1; k<per_list.size(); k++) {
                    def n1 = per_list[j]
                    def n2 = per_list[k]
                    Edge e = existing_edge(n1, n2)
                    if (e == null) {
                        e = db.graph.addEdge(n1, n2, L_RELATED_ENTITY)
                        // println "adding edge $e"
                        edge_count += 1
                        e['num_docs'] = 1
                    } else {
                        // println "num_docs++ on edge $e"
                        e['num_docs'] += 1
                    }
                }
            }

            db.commit()
            i += 1
        }
        println ("Added ${edge_count} edges")
    }
}
