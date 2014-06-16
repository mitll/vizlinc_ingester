import static edu.mit.ll.vizlincdb.util.VizLincProperties.*
import edu.mit.ll.vizlincdb.graph.VizLincDB

import com.tinkerpop.blueprints.impls.tg.*
import com.tinkerpop.blueprints.impls.neo4j.*
import com.tinkerpop.gremlin.groovy.*
import com.tinkerpop.blueprints.*
import com.tinkerpop.gremlin.*

// Create a Gephi GRAPHML file -- might create isolates
// Adapted from sn_gephi_gdf
class SNGephiGraphML {

    static NODE_MIN_DOC_REF = 2
    static EDGE_MIN_DOC_REF = 2

    // Fields
    VizLincDB db

    SNGephiGraphML(graphdb) {
        db = graphdb
    }

    static main(args)  {
        Gremlin.load()
        if (args.size() < 2) {
            println "Usage: SNGephiGraphML.groovy <db_name> <outfile.graphml>"
            println "Retrieve and filter the social network from the database and save to the GRAPHML file format."
            println "A person must be mentioned in at least $NODE_MIN_DOC_REF documents."
            println "Two people must be mentioned together in at least $EDGE_MIN_DOC_REF documents."
            System.exit(1)
        }

        def graphdb = new VizLincDB(args[0])
        SNGephiGraphML snGephiGraphML = new SNGephiGraphML(graphdb)
        snGephiGraphML.generate(args[1])
        graphdb.shutdown()
    }

    void generate(graphmlFilename) {
        def outfile = new FileWriter(graphmlFilename)
        outfile.write('''\
<?xml version="1.0" encoding="UTF-8"?>
<graphml xmlns="http://graphml.graphdrawing.org/xmlns">
 <key id="label" for="node" attr.name="label" attr.type="string"/>
 <key id="intid" for="node" attr.name="intid" attr.type="int"/>
 <key id="intid" for="edge" attr.name="intid" attr.type="int"/>
 <graph id="G" edgedefault="undirected">
''')

        // Query for frequent entity nodes
        def per_nodes = db.graph.V(P_CREATED_BY,'across_doc_person_coref').filter{it.getProperty('num_docs') >= NODE_MIN_DOC_REF}.sort{it.getProperty('entity_text')}
        def per_final = new HashSet()
        def node_count = 0
        for (per in per_nodes) {
            node_count += 1
            def per_name = per['entity_text']
            // if (!(per_name ==~ /[A-Z\s\.]+/)) {
            //    println "Unusual entity string, skipping: |" + per_name + "|"
            //    continue
            // }
            def per_id = per.getId()
            per_final.add(per_id)
            outfile.write("""\
              <node id="${per.getId()}"><data key="label">${per_name}</data><data key="intid">${per_id}</data></node>
""")
        }

        // Edges
        def edge_iter = db.graph.E.has('label', L_RELATED_ENTITY)._().filter{it.getProperty('num_docs') >= EDGE_MIN_DOC_REF}
        def total_edge_count = 0
        def included_edge_count = 0
        for (edge in edge_iter) {
            if ((total_edge_count % 10000)==0) {
                println "At edge: " + total_edge_count
            }
            total_edge_count += 1

            def edge_id = edge.getId()
            def p1 = edge.inV.next()
            def p2 = edge.outV.next()
            def p1_id = p1.getId()
            def p2_id = p2.getId()
            if (per_final.contains(p1_id) && per_final.contains(p2_id)) {
                included_edge_count += 1
                outfile.write("""\
  <edge id="${edge_id}" source="${p1_id}" target="${p2_id}"><data key="intid">${edge_id}</data></edge>
""")
            }
        }

        outfile.write('''\
  </graph>
 </graphml>
''')

        println("Total nodes: ${node_count}")
        println("Total edges: ${total_edge_count}")
        println("Included edges: ${included_edge_count}")

        outfile.close()
    }

}