import static edu.mit.ll.vizlincdb.util.VizLincProperties.*
import edu.mit.ll.vizlincdb.graph.VizLincDB

import com.tinkerpop.blueprints.impls.tg.*
import com.tinkerpop.blueprints.impls.neo4j.*
import com.tinkerpop.gremlin.groovy.*
import com.tinkerpop.blueprints.*
import com.tinkerpop.gremlin.*

//
// Get node Ids for nodes in Graph DB
//
class SNNodeId {

    static NODE_MIN_DOC_REF = 1

    // Fields
    VizLincDB db

    SNNodeId(graphdb) {
        db = graphdb
    }

    static main(args)  {
        Gremlin.load()
        if (args.size() < 2) {
            println "Usage: SNNodeId.groovy <db_name> <outfile>"
            println "Get the node Ids for all nodes in the graph DB."
            System.exit(1)
        }

        def graphdb = new VizLincDB(args[0])
        SNNodeId snNodeId = new SNNodeId(graphdb)
        snNodeId.saveNodeIds(args[1])
        graphdb.shutdown()
    }

    void saveNodeIds(outFile) {
        def outfile = new FileWriter(outFile)

        // Query for frequent entity nodes
        def per_nodes = db.graph.V(P_CREATED_BY,'across_doc_person_coref').filter{it.getProperty('num_docs') >= NODE_MIN_DOC_REF}.sort{it.getProperty('entity_text')}
        for (per in per_nodes) {
            def per_name = per['entity_text']
            def per_id = per.getId()
            outfile.write(per_id + "\t" + per_name + "\n")
        }

        per_nodes = db.graph.V(P_CREATED_BY,'within_doc_person_coref').filter{it.getProperty('num_docs') >= NODE_MIN_DOC_REF}.sort{it.getProperty('entity_text')}
        for (per in per_nodes) {
            def per_name = per['entity_text']
            def per_id = per.getId()
            outfile.write(per_id + "\t" + per_name + "\n")
        }
        outfile.close()

    }

}
