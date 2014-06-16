import static edu.mit.ll.vizlincdb.util.VizLincProperties.*
import edu.mit.ll.vizlincdb.util.ElapsedTime
import edu.mit.ll.vizlincdb.document.VizLincDocumentIndexer
import edu.mit.ll.vizlincdb.graph.VizLincDB
import edu.mit.ll.vizlincdb.io.VizLincGraphPopulator

import com.tinkerpop.blueprints.impls.neo4j.*
import com.tinkerpop.gremlin.groovy.*
import com.tinkerpop.blueprints.*
import com.tinkerpop.gremlin.*

import groovy.io.FileType
import java.nio.file.Path
import org.apache.commons.io.FilenameUtils




// Complete ingestion pipeline:

class Ingester {

    static final DEFAULT_DATA_DIR = 'data'
    static final DEFAULT_MODEL_NAME = 'ner-model.ser.gz'

    static final TXT_EXTENSION = '.txt'
    static final NER_EXTENSION = '.ner.csv'

    Ingester() {
    }

    enum Step {
        EXTRACT_TEXT('Extract text'),
        FIND_NAMED_ENTITIES('Find named entities'),
        POPULATE_GRAPH_DB('Populate graph database with documents and entity mentions'),
        PROCESS_PEOPLE('Process people (including social network)'),
        PROCESS_ORGANIZATIONS('Process organizations'),
        PROCESS_LOCATIONS('Process locations'),
        GEOCODE('Geocode locations'),
        PRECOMPUTE_COUNTS('Precompute counts'),
        INDEX_DOCS('Index documents'),
        CONVERT_TO_H2('Convert to H2 database')

        private final String text
        Step(String text) {
            this.text = text
        }

        public String text() {
            return text
        }
        
        public String toString() {
            return text
        }
    }


    // For running standalone.
    static main(args) {
        Gremlin.load()
        def cli = new CliBuilder(usage: 'Ingester.groovy [options]')
        cli.with {
            h(longOpt: 'help', 'show usage')
            i(longOpt: 'input-documents-dir', required: true, args: 1, argName: 'dir',
              'original documents dir (.pdf, etc.)')
            o(longOpt: 'output-dir',  required: true, args: 1, argName: 'dir',
              'dir for final .h2, .lucene, and .graphml files')
            w(longOpt: 'work-dir',  required: true, args: 1, argName: 'dir',
              'dir for intermediate results; WARNING: existing content may be deleted or overwritten')
            m(longOpt: 'model-filename',  required: false, args: 1, argName: 'model',
              "Stanford NER model filename (default: '<data-dir>/${DEFAULT_MODEL_NAME}')")
            b(longOpt: 'basename',  required: true, args: 1, argName: 'basename',
              'basename for the .neo4j, .graphml, .h2, and .lucene files and dirs')
            d(longOpt: 'data-dir',  required: false, args: 1, argName: 'dir',
              "dir for model and geocoding data files (default: '${DEFAULT_DATA_DIR}')")
        }

        def options = cli.parse(args)
        if (!options) return
        if (!options || options.h) {
            cli.usage()
            return
        }

        // Default for data-dir is 'data'.
        def dataDir = new File(options.'data-dir' ? options.'data-dir' : DEFAULT_DATA_DIR)
        def modelFile = options.'model-filename' ? new File(options.'model-filename') : new File(DEFAULT_DATA_DIR, DEFAULT_MODEL_NAME)

        Ingester ingester = new Ingester()
        ingester.ingest(options.'basename',
                        new File(options.'input-documents-dir'),
                        new File(options.'output-dir'),
                        new File(options.'work-dir'),
                        dataDir,
                        modelFile)
    }


    // Full ingestion pipeline
    void ingest(String basename, File originalDocumentsDir, File outputDir, File workDir, File dataDir, File nerModelFilename, Closure startStep = { step -> }, Closure stopStep = { step -> }) {

        if (!outputDir.exists()) {
            outputDir.mkdirs()
        }

        if (!workDir.exists()) {
            workDir.mkdirs()
        }

        def totalElapsedTime = new ElapsedTime()

        // Convenience function.
        def runStep = { step, code ->
            def elapsedTime = new ElapsedTime()
            println "*** ${step.text()}"
            startStep(step)
            code()
            stopStep(step)
            elapsedTime.done("**** finished ${step.text()} in (secs)")
        }
            
        def txtDir = new File(workDir, basename + "${TXT_EXTENSION}-files")
        runStep(Step.EXTRACT_TEXT, {
                    extractTextFromDocuments(originalDocumentsDir, txtDir, filesOnPath(originalDocumentsDir, ''), TXT_EXTENSION)
                })
                    
        def nerDir = new File(workDir, basename + "${NER_EXTENSION}-files")
        runStep(Step.FIND_NAMED_ENTITIES, {
                    markNamedEntities(txtDir, nerDir, filesOnPath(txtDir, TXT_EXTENSION), nerModelFilename, NER_EXTENSION)
                })

        def dbPath = new File(workDir, basename + '.neo4j')
        if (dbPath.exists()) {
            // Delete directory tree, or file if it was just a file.
            if (!dbPath.deleteDir()) {
                dbPath.delete()
            }
        }

        // VizLincGraphPopulator takes a dbPath, not a db instance, because it opens the db as a BatchGraph database for speed.
        runStep(Step.POPULATE_GRAPH_DB, {
                    def graphPopulator = new VizLincGraphPopulator(dbPath.getPath())
                    try {
                        addDocumentsAndMentionsToGraphDB(graphPopulator, txtDir, filesOnPath(txtDir, TXT_EXTENSION),
                                                         TXT_EXTENSION, nerDir, NER_EXTENSION)
                    } finally {
                        // Shutdown even on error (e.g. file not found). If not the database can be left in a bad state.
                        graphPopulator.shutdown()
                    }
                })

        // Now open the database as a transactional graph db.
        def db = new VizLincDB(dbPath)
        try {
            runStep(Step.PROCESS_PEOPLE, {
                        processPeople(db, new File(outputDir, basename + '.graphml'))
                    })

            runStep(Step.PROCESS_ORGANIZATIONS, {
                        processOrganizations(db)
                    })

            runStep(Step.PROCESS_LOCATIONS, {
                        processLocations(db)
                    })

            runStep(Step.GEOCODE, {
                        geocodeLocations(db)
                    })

            runStep(Step.PRECOMPUTE_COUNTS, {
                        computeCounts(db)
                    })

            runStep(Step.INDEX_DOCS, {
                        indexDocuments(db, new File(outputDir, basename + '.lucene'))
                    })

            // When all done with db:
            runStep(Step.CONVERT_TO_H2, {
                        convertToH2(db, new File(outputDir, basename + '.h2'))
                        println "Committing neo4j db ..."
                        db.commit()
                        println "... finished committing"
                    })

        } finally {
            println "Shutting down neo4j db ..."
            db.shutdown()
            println "... finished shutting down"
            totalElapsedTime.done("**** Entire ingestion finished in (secs)")
        }
    }


    List filesOnPath(File path, String extension) {
        def filenames = []
        Path rootPathObj = path.toPath()
        path.eachFileRecurse(FileType.FILES) {
            if (it.name.endsWith(extension)) {
                filenames.add(rootPathObj.relativize(it.toPath()).toString())
            }
        }
        
        return filenames.sort()
    }


    void extractTextFromDocuments(File originalDocumentsDir, File txtDir, List documentFileNames, String txtFilesExtension) {
        if (txtDir.exists()) {
            println "Deleting existing $txtDir"
            txtDir.deleteDir()
        }
        txtDir.mkdirs();

        TextExtractor textExtractor = new TextExtractor()
        for (docFileName in documentFileNames) {
            println "Extracting text from: ${docFileName}"
            def outputFile = new File(txtDir, docFileName + txtFilesExtension)
            outputFile.getParentFile().mkdirs()      // Create any necessary subdirs.
            textExtractor.extractText(new File(originalDocumentsDir, docFileName), outputFile)
        }
    }


    void markNamedEntities(File txtDir, File nerDir, List txtFileNames, File modelFile, String nerFilesExtension) {
        if (nerDir.exists()) {
            println "Deleting existing $nerDir"
            nerDir.deleteDir()
        }
        nerDir.mkdirs();

        EntityExtractor entityExtractor = new EntityExtractor(modelFile)
        for (txtFileName in txtFileNames) {
            println "finding named entities in: ${txtFileName}"
            def outputFile = new File(nerDir, txtFileName + nerFilesExtension)
            outputFile.getParentFile().mkdirs()      // Create any necessary subdirs.
            entityExtractor.extractEntitiesToCSV(new File(txtDir, txtFileName), outputFile)
        }
    }


    void addDocumentsAndMentionsToGraphDB(VizLincGraphPopulator populator, File txtDir, List txtFileNames, String txtExtension, File nerDir, String nerExtension) {
        for (txtFileName in txtFileNames) {
            String nerFileName = txtFileName + nerExtension
            println "populating graph with document: ${txtFileName} and mentions: ${nerFileName}"
            File txtFile = new File(txtDir, txtFileName)
            populator.ingestDocumentAndMentions(new File(txtDir, txtFileName), txtFileName, new File(nerDir, nerFileName))
        }
    }


    void processPeople(VizLincDB db, File graphmlFilename) {
        // Find person coreferences within and across docs.
        (new WithinDocPerCoref(db)).findCoreferences()
        (new AcrossDocPerCoref(db)).findCoreferences()

        // Compute the person social network, based on co-occurences across docs.
        (new SocialNetworkDCO(db)).computeSocialNetwork()

        // Convert the social network to a graph, and filter it somewhat.
        (new SNGephiGraphML(db)).generate(graphmlFilename.getPath())
    }

    void processOrganizations(VizLincDB db) {
        // Find organization coreferences within and across docs.
        (new WithinDocOrgCoref(db)).findCoreferences()
        (new AcrossDocOrgCoref(db)).findCoreferences()
    }

    void processLocations(VizLincDB db) {
        // Find location coreferences within and across docs.
        (new WithinDocLocCoref(db)).findCoreferences()
        (new AcrossDocLocCoref(db)).findCoreferences()
    }

    void geocodeLocations(VizLincDB db) {
        // Try to geocode all locations.
		println("geocode locations")
        (new Geocoder(db)).geocode()
    }

    void computeCounts(VizLincDB db) {
        // Count num mentions and put on edges.
        (new EdgeMentionsCounter(db)).countMentions()
    }

    void indexDocuments(VizLincDB db, File indexPath) {
        // Generate a lucene index from the documents in the database.
        VizLincDocumentIndexer indexer = new VizLincDocumentIndexer(indexPath.getPath())
        indexer.indexDocuments(db)
        indexer.close()
    }

    void convertToH2(VizLincDB db, File h2dbFile) {
        // Convert the final graph db to an H2 db.
        (new GraphDBToH2(db)).convert(h2dbFile.getPath())
    }
}
