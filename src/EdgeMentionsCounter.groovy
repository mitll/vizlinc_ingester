import static edu.mit.ll.vizlincdb.util.VizLincProperties.*
import edu.mit.ll.vizlincdb.graph.VizLincDB
import com.tinkerpop.blueprints.impls.neo4j.*
import com.tinkerpop.gremlin.groovy.*
import com.tinkerpop.blueprints.*
import com.tinkerpop.gremlin.*

// Add num_mentions property on document_entity edges

class EdgeMentionsCounter {

    // Fields
    VizLincDB db

    EdgeMentionsCounter(VizLincDB graphdb) {
        db = graphdb
    }

    // For running standalone.
    static main(args) {
        Gremlin.load()
        if (args.size() != 1) {
            println "Usage: EdgeMentionsCounter.groovy <db_name>"
            System.exit(1)
        }
        def db = new VizLincDB(args[0])
        EdgeMentionsCounter emc = new EdgeMentionsCounter(db)
        emc.countMentions()
        db.shutdown()
    }


    void countMentions() {
        def i = 0
        // def docs = db.getDocuments()._().order{it.a.getProperty(P_DOCUMENT_NAME) <=> it.b.getProperty(P_DOCUMENT_NAME)}
        def docs = db.getDocuments()

        // Add in counts
        for (doc in docs) {
            i += 1
            if (i % 100 == 0) {
                println "Committing at $i documents, at ${doc[P_DOCUMENT_NAME]}"
                db.commit()
            }

            // Get counts as assoc array
            // def doc_entity_count = [:]
            // doc.out(L_DOCUMENT_TO_ENTITY).transform{doc_entity_count[it.id]=0}.count()
            // doc.out('document_to_mention').out('mention_to_entity').transform{doc_entity_count[it.id]++}.count()

            // Now save counts to graph
            // doc.outE(L_DOCUMENT_TO_ENTITY).transform{it.inV.toList()[0][P_NUM_MENTIONS]=doc_entity_count[it.inV.toList()[0].id]}.count()

            // Get counts as assoc array and save to doc->entity edge (simpler version)
            def doc_entity_count = doc.out(L_DOCUMENT_TO_MENTION).out(L_MENTION_TO_ENTITY).groupCount{it.id}.cap.next()
            doc.outE(L_DOCUMENT_TO_ENTITY).sideEffect{def n=it.inV.toList()[0]; it[P_NUM_MENTIONS]=doc_entity_count[n.id]}.count()
        }
        println "Last commit..."
        db.commit()
    }

    void OLDcountMentions() {
        def i = 0
        def docs = db.getDocuments()._().order{it.a.getProperty(P_DOCUMENT_NAME) <=> it.b.getProperty(P_DOCUMENT_NAME)}
        for (doc in docs) {
            i += 1
            if (i % 100 == 0) {
                println "Committing at $i documents, at ${doc[P_DOCUMENT_NAME]}"
                db.commit()
            }

            def doc_entity_edges = doc.outE(L_DOCUMENT_TO_ENTITY)
            for (doc_entity_edge in doc_entity_edges) {
                def entity = doc_entity_edge.inV.toList()[0]   // inV is the vertex that edge is going into (confusing terminology)
                def matching_mentions_count = entity.in(L_MENTION_TO_ENTITY).in(L_DOCUMENT_TO_MENTION).filter{it == doc}.count()
                // println "${doc[P_DOCUMENT_NAME]} ${entity[P_ENTITY_TEXT]} $matching_mentions_count"
                doc_entity_edge.setProperty(P_NUM_MENTIONS, matching_mentions_count)
            }
        }

        println "Last commit..."
        db.commit()
    }
}