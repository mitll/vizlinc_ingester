import static edu.mit.ll.vizlincdb.util.VizLincProperties.*
import edu.mit.ll.vizlincdb.graph.VizLincDB
import com.tinkerpop.blueprints.impls.neo4j.*
import com.tinkerpop.gremlin.groovy.*
import com.tinkerpop.blueprints.*
import com.tinkerpop.gremlin.*

// Across document coref for given entity type

class AcrossDocSimpleCoref extends CorefBase {

    String entityType           // One of E_PERSON, E_LOCATION, etc.

    AcrossDocSimpleCoref(String entityType, VizLincDB graphdb) {
        super(graphdb)
        this.entityType = entityType
    }

    // For running standalone.
    static main(args) {
        Gremlin.load()
        if (args.size() != 2) {
            println "Usage: AcrossDocSimpleCoref.groovy <entity_type> <db_name>"
            System.exit(1)
        }
        def entityType = args[0]
        def db = new VizLincDB(args[1])
        AcrossDocSimpleCoref coref = new AcrossDocSimpleCoref(entityType, db)
        coref.findCoreferences()
        db.shutdown()
    }


    def isCandidate (name, num_tokens_min, num_tokens_max, num_chars_min) {
        if (name.length() < num_chars_min)
            return false

        def num_tokens = name.split().size()
        if ((num_tokens < num_tokens_min) || (num_tokens > num_tokens_max))
            return false

		  if (!(name==~/[A-Za-z\.\s]+/))
            return false

        return true
    }

    void findCoreferences(Boolean useGlobalID=false) {
        final MIN_NUM_DOCS = 2
        final DEBUG = false

		  println "In findCoreferences for AcrossDoc stuff for entity type " + entityType + " ..."

        // Query for entity nodes
        def nodes = db.getEntitiesOfType(entityType)

        // First pass -- get potential merge candidates
        def candidatesList = []
        def i = 0
        for (node in nodes) {
            def text = node.getProperty(P_ENTITY_TEXT)
            def nodeAndText = new NodeAndText(node, text)
            candidatesList.add(nodeAndText);
            i += 1
            if ((i % 10000) == 0) println "At entity #: " + i
        }
        candidatesList = candidatesList.sort{it.text}

        // Create clusters
        println "Length of candidatesList: " + candidatesList.size()
        def clusters
        if (useGlobalID) {
            clusters = combineByGlobalID(candidatesList)
        } else {
            clusters = combineByExactMatch(candidatesList)
        }
        println "Exact match, number of clusters: ${clusters.size()}"

        if (DEBUG) {
            println "clusters:"
            outputClusters(clusters)
        }

        // Filter clusters based on MIN_NUM_DOCS.
        def remove_list = []
        for (c in clusters) {
            def sz = c.value.size()
            if (sz < MIN_NUM_DOCS) {
                remove_list.add(c.key)
            }
        }
        for (ky in remove_list) {
            clusters.remove(ky)
        }

        println "Number of remaining clusters = " + clusters.size()

        // Merge clusters
        println "Merging clusters"
        // mergeClusters (clusters, "across_doc_${entityType.toLowerCase()}_coref".toString())
        mergeClusters (clusters, "across_doc_" + entityType.toLowerCase() + "_coref")
        db.commit()
    }

}