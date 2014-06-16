import static edu.mit.ll.vizlincdb.util.VizLincProperties.*
import edu.mit.ll.vizlincdb.graph.VizLincDB
import com.tinkerpop.blueprints.impls.neo4j.*
import com.tinkerpop.gremlin.groovy.*
import com.tinkerpop.blueprints.*
import com.tinkerpop.gremlin.*
import com.wcohen.ss.*

// Across document coref for person tags

class AcrossDocOrgCoref extends CorefBase {

    AcrossDocOrgCoref(VizLincDB graphdb) {
        super(graphdb)
    }

    // For running standalone.
    static main(args) {
        Gremlin.load()
        if (args.size() != 1) {
            println "Usage: AcrossDocOrgCoref.groovy <db_name>"
            System.exit(1)
        }
        def db = new VizLincDB(args[0])
        AcrossDocOrgCoref coref = new AcrossDocOrgCoref(db)
        coref.findCoreferences()
        db.shutdown()
    }

    def isCandidate (name, num_tokens_min, num_tokens_max, num_chars_min)
    {
        if (name.length() < num_chars_min)
            return false

        def num_tokens = name.split().size()
        if ((num_tokens < num_tokens_min) || (num_tokens > num_tokens_max))
            return false

        return true
    }


    void findCoreferences() {
        // Tunable parameters
        final NUM_TOKENS_MIN = 1
        final NUM_TOKENS_MAX = 20
        final NUM_CHARS_MIN = 2
        final MIN_NUM_DOCS = 2
        def DIST = new Levenstein()
        final THRESH = -1.1

        def DEBUG = false

        // Query for organization nodes
        def nodes = db.getEntitiesOfType(E_ORGANIZATION)

        // First pass -- get potential merge candidates
        def strong_candidates_list = []
        def weak_candidates_list = []
        def i = 0
        for (node in nodes) {
            def text = node[P_ENTITY_TEXT]
            def nodeAndText = new NodeAndText(node, text)
            if (isCandidate(text, NUM_TOKENS_MIN, NUM_TOKENS_MAX, NUM_CHARS_MIN)) {
                strong_candidates_list.add(nodeAndText);
            } else {
                weak_candidates_list.add(nodeAndText);
            }
            i += 1
            if ((i % 10000) == 0) println "At entity #: " + i
        }
        strong_candidates_list = strong_candidates_list.sort{it.text}
        weak_candidates_list = weak_candidates_list.sort{it.text}

        // Create clusters
        println "Length of strong_candidates_list: " + strong_candidates_list.size()
        def strong_clusters = combineByExactMatch(strong_candidates_list)
        println "Length of weak_candidates_list: " + weak_candidates_list.size()
        def weak_clusters = combineByExactMatch(weak_candidates_list)
        println "Exact match, number of strong clusters: " + strong_clusters.size()
        println "Exact match, number of weak clusters: " + weak_clusters.size()
        combineAdjacentClose(strong_clusters, DIST, THRESH, true)
        println "Close match, number of strong clusters: " + strong_clusters.size()

        if (DEBUG) {
            println "Strong clusters:"
            output_clusters(strong_clusters)
            println "\nWeak clusters:"
            output_clusters(weak_clusters)
        }


        // Filter strong clusters. Weak clusters are not filtered based on MIN_NUM_DOCS.
        def remove_list = []
        for (c in strong_clusters) {
            def sz = c.value.size()
            if (sz < MIN_NUM_DOCS) {
                remove_list.add(c.key)
            }
        }
        for (ky in remove_list) {
            strong_clusters.remove(ky)
        }

        println "Number of remaining strong clusters = " + strong_clusters.size()

        // Merge clusters
        println "Merging clusters"
        mergeClusters(strong_clusters, 'across_doc_organization_coref')
        mergeClusters(weak_clusters, 'weak_across_doc_organization_coref')

        db.commit()
    }

}

