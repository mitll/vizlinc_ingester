import com.ctc.wstx.stax.WstxInputFactory;
import edu.stanford.nlp.ling.Word;
import edu.stanford.nlp.process.PTBTokenizer;
import edu.stanford.nlp.process.WordTokenFactory;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.text.ParseException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import org.apache.commons.io.FilenameUtils;
import org.codehaus.stax2.XMLInputFactory2;
import org.codehaus.stax2.XMLStreamReader2;

/**
 * XMLToTSVConverter converts true XML entity-marked transcripts to a Stanford NER .tsv file.
 * .tsv is "tab-separated value".
 * Example:
 * <DOC><PERSON TYPE="NONE">Sam</PERSON> went to <LOCATION TYPE="NONE>New York</LOCATION>.</DOC>
 * becomes:  (the white space is supposed to be a single \t tab character)
 * Sam     PERSON
 * went    O
 * to      O
 * New     LOCATION
 * York    LOCATION
 * .       O
 *
 * @author DA21777
 */
public class XMLToTSVConverter {

    /**
     * Allowed entity tags. These tags always have one attribute, "TYPE"
     */
    Set<String> entityTags = new HashSet<>();
    String otherTag;

    /**
     * An entity parser that handles PERSON, ORGANIZATION, LOCATION, DATE
     */
    public XMLToTSVConverter() {
        this(["PERSON", "ORGANIZATION", "LOCATION", "DATE"], "O");
    }

    /**
     * Handle the entities given in the entityTags list.
     *
     * entityTags tags
     * @param entityTags
     * @param otherTag
     */
    public XMLToTSVConverter(String[] entityTags, String otherTag) {
        Collections.addAll(this.entityTags, entityTags);
        this.otherTag = otherTag;
    }

    public void convertToTSV(File inputFile, File outputFile) throws IOException, ParseException {
        BufferedReader input = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile), "UTF-8"));
        BufferedWriter output = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), "UTF-8"));
        convertToTSV(input, output);
        input.close();
        output.close();

    }

    public String convertToTSV(String s) throws ParseException, IOException {
        BufferedWriter output = new BufferedWriter(new StringWriter());
        convertToTSV(new StringReader(s), output);
        return output.toString();
    }

    public void convertToTSV(Reader reader,  BufferedWriter writer) throws ParseException, IOException {
        try {
            // Matches <SOMETHING TYPE="WHATEVER"> or </SOMETHING>
            // where SOMETHING is PERSON|ORGANIZATION|LOCATION|DATE.
            // There may be other entityTags in the document but they are ignored. Phoenix adds these, for instance:
            // DOC DOCID DOCTYPE BODY TEXT

            XMLInputFactory2 factory = new WstxInputFactory();
            factory.configureForConvenience();
            // Woodstox
            XMLStreamReader2 xmlReader = (XMLStreamReader2) factory.createXMLStreamReader(reader);

            // Don't output until the <TEXT> section.
            boolean inTEXT = false;
            String currentTag = otherTag;
            StringBuilder text = new StringBuilder();

            while (xmlReader.hasNext()) {
                int eventType = xmlReader.next();
                switch (eventType) {
                    case XMLStreamConstants.START_ELEMENT:
                        String name = xmlReader.getLocalName();
                        if (name.equals("TEXT")) {
                            // Don't converting until we're actually in the document <TEXT> section.
                            inTEXT = true;
                        } else if (inTEXT && entityTags.contains(name)) {
                            writeTextTokens(writer, text.toString(), currentTag);
                            text = new StringBuilder();
                            currentTag = name;
                        }
                        break;

                    case XMLStreamConstants.CHARACTERS:
                    case XMLStreamConstants.CDATA:
                        // Accumulate text between or outside of tags.
                        if (inTEXT) {
                            text.append(xmlReader.getText());
                        }
                        break;

                    case XMLStreamConstants.END_ELEMENT:
                        // If this is a tag we care about, it's closed, and we have all the text inside it.
                        if (inTEXT && entityTags.contains(xmlReader.getLocalName())) {
                            writeTextTokens(writer, text.toString(), currentTag);
                            text = new StringBuilder();
                            currentTag = otherTag;
                        }
                        break;

                    // We don't care about all other states.
                    default:
                        break;
                }
            }


        } catch (XMLStreamException ex) {
            Logger.getLogger(XMLToTSVConverter.class.getName()).log(Level.SEVERE, null, ex);
            throw new ParseException(ex.getMessage(), ex.getLocation().getCharacterOffset());

        }
    }

    private void writeTextTokens(BufferedWriter writer, String text, String currentTag) throws IOException {
        // System.out.println(text + " --- " + currentTag);
        Reader reader = new StringReader(text);
        PTBTokenizer<Word> tokenizer = new PTBTokenizer(reader, new WordTokenFactory(), /*options*/ "");
        while (tokenizer.hasNext()) {
            Word word = tokenizer.next();
            writer.append(word.word());
            writer.append('\t');
            writer.append(currentTag);
            writer.newLine();
        }
    }

    /**
     * @param args args[0]: output directory; args[1..]: input file ...
     * @throws java.io.IOException
     * @throws java.text.ParseException
     */
    public static void main(String[] args) throws IOException, ParseException {
        if (args.length < 2) {
            System.out.println("Usage: XMLToTSVConverter.groovy <output-directory> <input-file> ...");
            return;
        }

        XMLToTSVConverter converter = new XMLToTSVConverter();
        File outputDir = new File(args[0]);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }
        for (int i = 1; i < args.length; i++) {
            String inputFilename = args[i];
            String basename = FilenameUtils.getBaseName(inputFilename);
            System.out.println("Processing " + inputFilename);
            converter.convertToTSV(new File(inputFilename), new File(outputDir, basename + ".tsv"));

        }
    }
}
