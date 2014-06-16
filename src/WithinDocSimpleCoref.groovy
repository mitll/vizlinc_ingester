import static edu.mit.ll.vizlincdb.util.VizLincProperties.*
import edu.mit.ll.vizlincdb.graph.VizLincDB

import com.tinkerpop.blueprints.impls.neo4j.*
import com.tinkerpop.gremlin.groovy.*
import com.tinkerpop.blueprints.*
import com.tinkerpop.gremlin.*

// Within document coref for given entity type

class WithinDocSimpleCoref extends CorefBase {

    String entityType           // One of E_PERSON, E_LOCATION, etc.

    WithinDocSimpleCoref(String entityType, VizLincDB graphdb) {
        super(graphdb)
        this.entityType = entityType
    }

    // For running standalone.
    static main(String[] args) {
        Gremlin.load()
        if (args.size() != 2) {
            println "Usage: WithinDocSimpleCoref.groovy <entity_type> <db_name>"
            System.exit(1)
        }
        def entityType = args[0]
        def db = new VizLincDB(args[1])
        WithinDocSimpleCoref coref = new WithinDocSimpleCoref(entityType, db)
        if (entityType==E_LOCATION)
            coref.findCoreferences(true)
        else 
            coref.findCoreferences(true)

        db.shutdown()
    }

    void findCoreferences(Boolean useGlobalID=false) {
        def i = 0
        def DEBUG = false

        if (DEBUG) {
            if (useGlobalID)
                println "Using global IDs for within doc coref."
            else
                println "Using exact string match for within doc coref."
        }

        def doc_nodes = db.getDocuments()
        for (node in doc_nodes) {
            println "Document node " + i + " : " + node

            def mentionList = getTwitterNormalizedMentions(node, entityType)

            if (DEBUG) { println "${entityType} list: ${mentionList}" }
            println "Number of names: " + mentionList.size()

            def clusters

            if (useGlobalID) {
                clusters = combineByGlobalID(mentionList)
                addEntitiesToGraphByGlobalID(clusters, entityType, "within_doc_${entityType.toLowerCase()}_coref")
            } else {
                clusters = combineByExactMatch(mentionList)
                addEntitiesToGraph(clusters, entityType, "within_doc_${entityType.toLowerCase()}_coref")
            }
            println "Exact match, number of clusters: " + clusters.size()
            if (DEBUG) { outputClusters(clusters) }

            print "\n\n"
            i += 1
        }
        db.commit()
    }
}

