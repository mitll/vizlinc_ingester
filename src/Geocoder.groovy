import static edu.mit.ll.vizlincdb.util.VizLincProperties.*
import edu.mit.ll.vizlincdb.graph.VizLincDB
import edu.mit.ll.vizlincdb.geo.GeoBoundingBox
import edu.mit.ll.vizlincdb.geo.GeoLocation

import com.tinkerpop.blueprints.impls.neo4j.*
import com.tinkerpop.gremlin.groovy.*
import com.tinkerpop.blueprints.*
import com.tinkerpop.gremlin.*

import java.util.Collections
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class Geocoder {

    // Annotate location entities with latitude, longitude, possibly other info.

    final static NUM2 = /\d\d/
    final static NUM2_WITH_DECIMAL = /$NUM2(?:[.,]\d*)?/
    final static SEP = /\D{1,9}/
    final static NOISE = /\D*/
    final static LAT_LONG_1 = ~ /^$NOISE($NUM2)$SEP($NUM2)$SEP($NUM2_WITH_DECIMAL)$SEP($NUM2)$SEP($NUM2)$SEP($NUM2_WITH_DECIMAL)$NOISE$/ 
    final static LAT_LONG_2 = ~ /^$NOISE($NUM2)($NUM2)($NUM2)$SEP($NUM2)($NUM2)($NUM2)$NOISE$/


    static final BAD_COORDINATE = -999.0

    // Fields
    VizLincDB db
    int locationCount = 0
    int numThreads
    List<GeoResult> geoResultList = Collections.synchronizedList(new ArrayList<GeoResult>())
    OSMFetcher osmFetcher = null
    
    class GeoResult {
        // Fields
        def node
        List locations

        GeoResult(node, locations) {
            this.node = node
            this.locations = locations
        }
    }

    Geocoder(VizLincDB db, numThreads=1) {
        this.db = db
        osmFetcher = new OSMFetcher()
        
    }



    String toDottedDecimal(num) {
        return num.replaceAll(",", ".");
    }

    // For running standalone.
    static main(args) {
		System.properties << [ 'http.proxyHost':'llproxy.llan.ll.mit.edu','http.proxyPort':'8080' ]

        Gremlin.load()
        def cli = new CliBuilder(usage: 'Geocoder.groovy [options] db_name\n')
        cli.with {
            h longOpt: 'help', 'Show usage'
            _ longOpt: 'coref-only',  'Use only locations that are marked with across_doc_location_coref'
        }

        def options = cli.parse(args)
        if (!options || options.h) {
            cli.usage()
            return
        }

        

        // Must specify exactly one non-option.
        def arguments = options.arguments()
        if (arguments == null || arguments.size() >  1) {
            println "ERROR: Specify exactly one database"
            cli.usage()
            return
        }

        def dbname = arguments[0]
        
        def db = new VizLincDB(dbname)
        Geocoder geocoder = new Geocoder(db)
        geocoder.geocode(options.'coref-only')
        db.shutdown()
    }

    // Try to parse as a literal lat, long string
    // Returns null if it can't parse -- need to lookup place name in this case
    def parse_lat_long(place) {
        // NUM could use . or , as a decimal point.
        // Don't make SEP too long or we'll pick up bogus 6-number strings as lat-longs.
        // All non-decimal coordinates are two digits.
        def match = place =~ LAT_LONG_1
        if (!match.matches()) match = place =~ LAT_LONG_2
        // if (!match.matches()) match = place =~ LAT_LONG_3 // etc.
        if (match.matches()) {
            def groups = match[0]
            def lat = groups[1].toDouble() + groups[2].toDouble()/60.0 + toDottedDecimal(groups[3]).toDouble()/3600.0
            def lon = -(groups[4].toDouble() + groups[5].toDouble()/60.0 + toDottedDecimal(groups[6]).toDouble()/3600.0)
            return [new GeoLocation(lat, lon, null, place, null, null, null, GEO_SOURCE_COORDINATES)]
        
        } else {
            return null
        }
    }

    def queueGeoLocations(node, geolocations) {
        geoResultList.add(new GeoResult(node, geolocations))
    }

    def storeGeoLocations(geoResult) {
        def node = geoResult.node
        print node.getProperty('entity_text')

        def geolocations = geoResult.locations
        if (geolocations.size() > 0) {
            println " ${geolocations[0].source}-${geolocations[0].country} ${geolocations.size}"
        } else {
            println " notfound"
            return;
        }

        // Don't store bad coordinate markers.
        if (geolocations[0].latitude == BAD_COORDINATE) {
            return;
        }

        db.setGeoLocations(node, geolocations as GeoLocation[])

        ++locationCount
        if (locationCount  % 1000 == 0) {
            println "Committing at ${locationCount}"
            db.commit()
        }
    }


    // Regexps for cleanup()
    final static LEADING_DASHES = /^[- ]/


    def cleanup(place) {
        return place.replaceFirst(LEADING_DASHES, '')

    }

    def geocodeOne(node) {
        def place = node.getProperty('entity_text')
        //println "geocoding $place"

        // First see if it's a specified lat/logn.
        def geolocations = parse_lat_long(place)
        if (geolocations != null) {
            // If size = 0, the string parsed OK, but no geolocations found 
            if (geolocations.size() != 0) {
                queueGeoLocations(node, geolocations)
                return
            }
        }

        // For lookup, clean up junk characters, etc.
        place = cleanup(place)
        if (place == '') {
            queueGeoLocations(node, [])
            return
        }

        // Look in OSM
        if (osmFetcher != null) {
            geolocations = osmFetcher.getGeoLocations(place)
            if (geolocations.size() != 0) {
                queueGeoLocations(node, geolocations)
                return
            }
        }




     }


    void geocode(boolean corefOnly=false) {


        def location_nodes = corefOnly ?
            db.getEntitiesCreatedBy("across_doc_location_coref").toList().sort{it['entity_text']} :
            db.getEntitiesOfType(E_LOCATION).toList().sort{it['entity_text']}

        for (node in location_nodes) {
            final finalNode = node
            // println "queuing ${node.getProperty('entity_text')}"
	    geocodeOne(finalNode)
        }



        // Store all the results.

        geoResultList.each { storeGeoLocations(it) }

        //db.rollback()
        db.commit()
        println "Processed: ${geoResultList.size()} locations"
    }
}
