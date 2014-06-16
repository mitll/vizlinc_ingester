import edu.stanford.nlp.ie.crf.CRFClassifier
import edu.stanford.nlp.util.Triple

import edu.mit.ll.vizlincdb.io.CSVMention
import edu.mit.ll.vizlincdb.io.CSVMentionFileWriter

class EntityExtractor {

    // fields
    CRFClassifier classifier

    public EntityExtractor(File nerModelFile) {
        classifier = CRFClassifier.getClassifier(nerModelFile)
    }


    static final DEFAULT_MODEL_NAME = 'ner-model.ser.gz'

    def extractEntitiesToCSV(File inputFile, File outputFile) {
        String text = inputFile.getText('UTF-8')
        CSVMentionFileWriter writer = new CSVMentionFileWriter(outputFile)
        int entityIndex = 0
        for (Triple<String, Integer, Integer> entityAndOffsets : classifier.classifyToCharacterOffsets(text)) {
            // Stanford NER replaces regular spaces inside XML tags with Unicode non-breaking spaces. Undo this.
            String mentionType = entityAndOffsets.first.replaceAll('\u00A0', ' ')
            int start = entityAndOffsets.second
            int stop = entityAndOffsets.third
            String mentionText = text.substring(start, stop)
            // We don't have a global id for the entity, so omit it (will read as NULL).
            writer.write(new CSVMention(mentionType, start, stop, entityIndex, null, mentionText))
            entityIndex++
        }
        writer.close();
    }


    static main(String[] args) {
        def cli = new CliBuilder(usage: 'EntityExtractor.groovy [options] file...')
        cli.with {
            h(longOpt: 'help', 'Show usage')
            m(longOpt: 'model',  required: false, args: 1, argName: 'model', "Stanford NER model filename (default: '${DEFAULT_MODEL_NAME}')")
            o(longOpt: 'output-dir', args: 1, argName: 'outputDir', required: true, 'write extracted text files into this directory')
        }

        def options = cli.parse(args)
        if (!options) return
        if (options.h) {
            cli.usage()
            return
        }
        def outputDir = new File(options.'output-dir')
        def arguments = options.arguments()
        if (arguments.size() == 0) {
            cli.usage()
            return
        }

        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        def entityExtractor = new EntityExtractor(new File(options.'model' ? options.'model' : DEFAULT_MODEL_NAME));
        for (filename in arguments) {
            def inputFile = new File(filename)
            println "Entity extraction: $filename"
            entityExtractor.extractEntitiesToCSV(new File(filename), new File(outputDir, inputFile.getName() + '.ner.csv'))
        }
    }
}
