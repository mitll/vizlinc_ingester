import static edu.mit.ll.vizlincdb.util.VizLincProperties.*
import edu.mit.ll.vizlincdb.graph.VizLincDB

import com.tinkerpop.blueprints.impls.neo4j.*
import com.tinkerpop.gremlin.groovy.*
import com.tinkerpop.blueprints.*
import com.tinkerpop.gremlin.*

import com.wcohen.ss.*

// Within document coref for person tags

class WithinDocPerCoref extends CorefBase {

    WithinDocPerCoref(VizLincDB graphdb) {
        super(graphdb)
    }

    // For running standalone.
    static main(String[] args) {
        Gremlin.load()
        if (args.size() != 1) {
            println "Usage: WithinDocPerCoref.groovy <db_name>"
            System.exit(1)
        }
        def db = new VizLincDB(args[0])
        WithinDocPerCoref coref = new WithinDocPerCoref(db)
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
            def person_list = getNormalizedMentions(node, E_PERSON)
            if (DEBUG) { println "Person list: " + person_list }
            println "Number of names: " + person_list.size()
            def clusters = combineByExactMatch(person_list)
            println "Exact string match, number of clusters: " + clusters.size()
            if (DEBUG) { outputClusters(clusters) }
            combineAdjacentClose(clusters, DIST, THRESH, false)
            println "Combine adjacent, number of clusters: " + clusters.size()
            if (DEBUG) { outputClusters(clusters) }
            combineFirstnameFullname(clusters)
            println "Combine first name, number of clusters: " + clusters.size()
            if (DEBUG) { outputClusters(clusters) }
            addEntitiesToGraph(clusters, E_PERSON, 'within_doc_person_coref')
            print "\n\n"
            i += 1
        }
        db.commit()
    }
}

