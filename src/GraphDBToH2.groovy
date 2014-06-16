import static edu.mit.ll.vizlincdb.util.VizLincProperties.*
import edu.mit.ll.vizlincdb.graph.VizLincDB
import edu.mit.ll.vizlincdb.relational.VizLincRDB

import groovy.sql.Sql
import java.sql.SQLException
import org.apache.commons.io.FileUtils
import com.tinkerpop.blueprints.impls.neo4j.*
import com.tinkerpop.gremlin.groovy.*
import com.tinkerpop.blueprints.*
import com.tinkerpop.gremlin.*

class GraphDBToH2 {

    // Fields
    def db  // h2 db object
    VizLincDB gdb


    GraphDBToH2(graphdb) {
        gdb = graphdb
    }


    // For running standalone.
    static main(args) {
        if (args.size() != 2) {
            println "Usage: GraphDBToH2.groovy <neo4j-graph> <h2-database-prefix>"
            System.exit(1)
        }

        def graphdb = new VizLincDB(args[0])
        GraphDBToH2 graphDBToH2 = new GraphDBToH2(graphdb)
        graphDBToH2.convert(args[1])
        println "shutting down graph db"
        graphdb.shutdown()
    }


    def convert(h2dbFilename) {
        def h2dbFile = new File(h2dbFilename)
        FileUtils.deleteQuietly(h2dbFile)
        h2dbFile.mkdir()
        db = Sql.newInstance("jdbc:h2:${h2dbFile}/data;LOG=0;CACHE_SIZE=0;LOCK_MODE=0;UNDO_LOG=0", 'sa', '', 'org.h2.Driver')

        // Create all the tables, indices, etc.
        db.execute(VizLincRDB.H2_SCHEMA)

        convertEntities()
        convertDocuments()
        convertEdges()
        convertMentions()

        println "closing h2 db..."
        db.close()
        println "... closed"
    }


    private void convertMentions() {
        gdb.getMentions().eachWithIndex { mention, count ->
            if (count % 1000 == 0) {
                println "mention $count"
                db.commit();
            }
            def mention_id = mention.getId()
            def document_id = VizLincDB.makeList(mention.getVertices(Direction.IN, L_DOCUMENT_TO_MENTION))[0].getId()
            def entities = VizLincDB.makeList(mention.getVertices(Direction.OUT, L_MENTION_TO_ENTITY))  // should be length 0 or 1
            def entity_id = (entities.size() == 1) ? entities[0].getId() : null
            def type = mention.getProperty(P_MENTION_TYPE)
            def text = mention.getProperty(P_MENTION_TEXT)
            def index = mention.getProperty(P_MENTION_INDEX)
            def global_id = mention.getProperty(P_MENTION_GLOBAL_ID)
            def text_start = mention.getProperty(P_MENTION_TEXT_START)
            def text_stop = mention.getProperty(P_MENTION_TEXT_STOP)

            try {
                // Triple-quoted Groovy Strings actually generate a prepared statement.
                db.execute("""INSERT INTO mention (mention_id, document_id, entity_id, type, text, index, global_id, text_start, text_stop)
                              VALUES(${mention_id}, ${document_id}, ${entity_id}, ${type}, ${text}, ${index}, ${global_id}, ${text_start}, ${text_stop}  )""")
            } catch (SQLException e) {
                e.printStackTrace()
                println("""VALUES: ${mention_id}, ${document_id}, ${entity_id}, ${type}, ${text}, ${index}, ${global_id}, ${text_start}, ${text_stop}""")
            }
        }
    }


    private void convertEdges() {
        gdb.getEdges(L_DOCUMENT_TO_ENTITY).eachWithIndex { edge, count ->
            if (count % 1000 == 0) {
                println "document->entity $count"
                db.commit();
            }
            def doc = edge.getVertex(Direction.OUT)
            def entity = edge.getVertex(Direction.IN)
            assert doc.getProperty(P_NODE_TYPE) == NODE_TYPE_DOCUMENT
            assert entity.getProperty(P_NODE_TYPE) == NODE_TYPE_ENTITY
            def document_id = doc.getId()
            def entity_id = entity.getId()
            def num_mentions = edge.getProperty(P_NUM_MENTIONS)
            // println ("doc ${document_id} entity ${entity_id} num_mentions ${num_mentions}")
            // Use MERGE because duplicate edges will cause key errors.
            db.execute("""MERGE INTO document_entity (document_id, entity_id, num_mentions)
                          VALUES(${document_id}, ${entity_id}, ${num_mentions})""")
        }
    }


    private void convertDocuments() {
        gdb.getDocuments().eachWithIndex { doc, count ->
            if (count % 1000 == 0) {
                println "adding document $count"
                db.commit();
            }
            def document_id = doc.getId()
            def name = doc.getProperty(P_DOCUMENT_NAME)
            def path = doc.getProperty(P_DOCUMENT_PATH)
            def text = doc.getProperty(P_DOCUMENT_TEXT)

            db.execute("""INSERT INTO document (document_id, name, path, text)
                          VALUES(${document_id}, ${name}, ${path}, ${text})""")
        }
    }


    private void convertEntities() {
        gdb.getEntities().eachWithIndex { entity, count ->
            if (count % 1000 == 0) {
                println "entity $count"
                db.commit();
            }

            def entity_id = entity.getId()
            def type = entity.getProperty(P_ENTITY_TYPE)
            def text = entity.getProperty(P_ENTITY_TEXT)
            def created_by = entity.getProperty(P_CREATED_BY)
            def num_documents = entity.getProperty(P_NUM_DOCUMENTS)
            def num_mentions = entity.getProperty(P_NUM_MENTIONS)
            def geolocations = gdb.getGeoLocations(entity)

            db.execute("""INSERT INTO entity (entity_id, type, text, created_by, num_documents, num_mentions)
                  VALUES(${entity_id}, ${type}, ${text}, ${created_by}, ${num_documents}, ${num_mentions})""")

            if (geolocations != null) {
                geolocations.eachWithIndex { loc, rank ->
                    def bbox = loc.boundingBox
                    if (bbox != null && bbox.isValid()) {
                        db.execute("""INSERT INTO geolocation (entity_id, rank, latitude, longitude,
                                                               latitude_south, latitude_north, longitude_west, longitude_east,
                                                               name, osm_type, nga_designation, country, source)
                                      VALUES(${entity_id}, ${rank}, ${loc.latitude}, ${loc.longitude},
                                             ${bbox.latitudeSouth}, ${bbox.latitudeNorth}, ${bbox.longitudeWest}, ${bbox.longitudeEast},
                                             ${loc.name}, ${loc.osmType}, ${loc.ngaDesignation}, ${loc.country}, ${loc.source})""")
                    } else {
                        db.execute("""INSERT INTO geolocation (entity_id, rank, latitude, longitude,
                                                               name, osm_type, nga_designation, country, source)
                                      VALUES(${entity_id}, ${rank}, ${loc.latitude}, ${loc.longitude},
                                             ${loc.name}, ${loc.osmType}, ${loc.ngaDesignation}, ${loc.country}, ${loc.source})""")

                    }
                }
            }
        }
    }


}