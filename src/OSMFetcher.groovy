import edu.mit.ll.vizlincdb.geo.GeoBoundingBox
import edu.mit.ll.vizlincdb.geo.GeoLocation

class OSMFetcher {


    static String baseURL = 'http://open.mapquestapi.com/nominatim/v1/search.php'

    def getGeoLocations(String location) {
    	URL mapquest = new URL("${baseURL}?format=xml&q=${URLEncoder.encode(location, 'UTF-8')}&addressdetails=1&limit=1")
        return getGeoLocationsFromURL(new URL("${baseURL}?format=xml&q=${URLEncoder.encode(location, 'UTF-8')}&addressdetails=1&limit=1"))
    }

    def getGeoLocationsInCountry(String countryCode, String location) {
        return getGeoLocationsFromURL(new URL("${baseURL}&addressdetails=1&format=xml&bounded=0&osm_type=N&q=${URLEncoder.encode(location, 'UTF-8')}"))
    }

    def getGeoLocationsFromURL(URL url) {
        try {
            def xml = url.getText()
            def path = new XmlSlurper().parseText(xml)
            assert path.name() == 'searchresults'
            return path['place'].collect { place ->
                def (latS, latN, lonW, lonE) = place['@boundingbox'].text().tokenize(',').collect{it.toDouble()}
                GeoLocation.OSMGeoLocation(
                    place['@lat'].toDouble(),place['@lon'].toDouble(),
                    new GeoBoundingBox(latS, latN, lonW, lonE),
                    place['@display_name'].text(),
                    place['@type'].text(),
                    place['country_code'].text())
            }
        } catch (IOException e) {
	    println("exception")
	    println(e)
            return []
        }
    }
}