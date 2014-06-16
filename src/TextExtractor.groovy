import org.apache.tika.Tika
import org.apache.tika.exception.TikaException

class TextExtractor {

    def tika = new Tika()

    public TextExtractor() {
    }


    def extractText(File inputFile, File outputFile) {
        BufferedWriter output = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), "UTF-8"));
        try {
            output.append(extractText(inputFile))
        } finally {
            output.close();
        }
    }

    
    def extractText(File inputFile) {
        try {
            return tika.parseToString(inputFile)
        } catch (TikaException e) {
            println "ERROR: ${inputFile.getPath()}: ${e.getMessage()}: ${e.getCause()}"
        }
    }


    static main(String[] args) {
        def cli = new CliBuilder(usage: 'TextExtractor.groovy [options] file...')
        cli.with {
            h(longOpt: 'help', 'Show usage')
            o(longOpt: 'output-dir', args: 1, argName: 'outputDir', required: true, 'write extracted text files into this directory (required)')
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

        def textExtractor = new TextExtractor();
        for (filename in arguments) {
            def inputFile = new File(filename)
            println "Extracting text: $filename"
            textExtractor.extractText(new File(filename), new File(outputDir, inputFile.getName() + '.txt'))
        }
    }
}
