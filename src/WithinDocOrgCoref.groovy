import static edu.mit.ll.vizlincdb.util.VizLincProperties.*
import edu.mit.ll.vizlincdb.graph.VizLincDB

import com.tinkerpop.blueprints.impls.neo4j.*
import com.tinkerpop.gremlin.groovy.*
import com.tinkerpop.blueprints.*
import com.tinkerpop.gremlin.*

import com.wcohen.ss.*

// Within document coref for organizations tags

class WithinDocOrgCoref extends CorefBase {

    WithinDocOrgCoref(VizLincDB graphdb) {
        super(graphdb)
    }

    // For running standalone.
    static main(args) {
        Gremlin.load()
        if (args.size() != 1) {
            println "Usage: WithinDocOrgCoref.groovy <db_name>"
            System.exit(1)
        }
        def db = new VizLincDB(args[0])
        WithinDocOrgCoref coref = new WithinDocOrgCoref(db)
        coref.findCoreferences()
        db.shutdown()
    }


    void findCoreferences() {
        def i = 0
        def DEBUG = false

        def DIST = new Levenstein()
        final THRESH = -1.1

        def doc_nodes = db.getDocuments()
        for (node in doc_nodes) {
            println "Document node " + i + " : " + node
            def organization_list = getNormalizedMentions(node, E_ORGANIZATION)
            if (DEBUG) { println "Organization list: " + organization_list }
            println "Number of names: " + organization_list.size()
            def clusters = combineByExactMatch(organization_list)
            println "Exact string match, number of clusters: " + clusters.size()
            if (DEBUG) { outputClusters(clusters) }
            combineAdjacentClose(clusters, DIST, THRESH, true)
            println "Combine adjacent, number of clusters: " + clusters.size()
            if (DEBUG) { outputClusters(clusters) }
            // combineFirstnameFullname(clusters)  // Not sure if this is a good thing for orgs
            // println "Combine first name, number of clusters: " + clusters.size()
            if (DEBUG) { outputClusters(clusters) }
            addEntitiesToGraph(clusters, E_ORGANIZATION, 'within_doc_organization_coref')
            print "\n\n"
            i += 1
        }

        db.commit()
    }
}

