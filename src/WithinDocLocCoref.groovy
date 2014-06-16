import static edu.mit.ll.vizlincdb.util.VizLincProperties.*
import edu.mit.ll.vizlincdb.graph.VizLincDB

import com.tinkerpop.blueprints.impls.neo4j.*
import com.tinkerpop.gremlin.groovy.*
import com.tinkerpop.blueprints.*
import com.tinkerpop.gremlin.*
import com.wcohen.ss.*

// Within document coref for location tags

class WithinDocLocCoref extends CorefBase {

    WithinDocLocCoref(VizLincDB graphdb) {
        super(graphdb)
    }

    // For running standalone.
    static main(args) {
        Gremlin.load()
        if (args.size() != 1) {
            println "Usage: WithinDocLocCoref.groovy <db_name>"
            System.exit(1)
        }
        def db = new VizLincDB(args[0])
        WithinDocLocCoref coref = new WithinDocLocCoref(db)
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
            def location_list = getNormalizedMentions(node, E_LOCATION, JUNK_CHARS_TO_REMOVE_FOR_LOCS)
            if (DEBUG) { println "Location list: " + location_list }
            println "Number of names: " + location_list.size()
            def clusters = combineByExactMatch(location_list)
            println "Exact string match, number of clusters: " + clusters.size()
            if (DEBUG) { output_clusters(clusters) }
            combineAdjacentClose(clusters, DIST, THRESH, true)
            println "Combine adjacent, number of clusters: " + clusters.size()
            if (DEBUG) { outputClusters(clusters) }
            combineFirstnameFullname(clusters)
            println "Combine first name, number of clusters: " + clusters.size()
            if (DEBUG) { outputClusters(clusters) }
            addEntitiesToGraph(clusters, E_LOCATION, 'within_doc_location_coref')
            print "\n\n"
            i += 1
        }
        db.commit()
    }

}
