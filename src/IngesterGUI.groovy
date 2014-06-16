// GUI interface for Ingester

import com.tinkerpop.gremlin.groovy.*

import groovy.ui.SystemOutputInterceptor
import groovy.swing.SwingBuilder
import java.awt.Color
import java.awt.Dimension
import javax.swing.BorderFactory
import javax.swing.JComponent
import javax.swing.JFileChooser
import javax.swing.JFrame
import javax.swing.JLabel
import javax.swing.JOptionPane
import javax.swing.ScrollPaneConstants
import javax.swing.text.DefaultCaret
import javax.swing.UIManager

class IngesterGUI {

    static final DEFAULT_DATA_DIR = 'data'

    // Fields
    File dataDir
    SwingBuilder swing
    def stepList
    def resultsTextArea
    def logTextArea
    def logScrollPane

    IngesterGUI(File dataDir) {
        this.dataDir = dataDir
    }

    static final DISALLOWED_BASENAME_CHARS = '\\/:*?"<>|\''

    def MODEL_FILENAME = 'ner-model.ser.gz'

    // Fields
    def uiFrame
    def inputFolderChooser
    def outputFolderChooser

    static main(args) {
        Gremlin.load()
        def cli = new CliBuilder(usage: 'IngesterGUI.groovy [options]')
        cli.with {
            h(longOpt: 'help', 'show usage')
            d(longOpt: 'data-dir',  required: false, args: 1, argName: 'dir',
              "dir for model and geocoding data files (default: '${DEFAULT_DATA_DIR}')")
        }

        def options = cli.parse(args)
        if (!options) return
        if (!options || options.h) {
            cli.usage()
            return
        }

        def dataDir = new File(options.'data-dir' ? options.'data-dir' : DEFAULT_DATA_DIR)
        IngesterGUI ingesterGUI = new IngesterGUI(dataDir)
        // Ingestion will run when the user presses the button
        ingesterGUI.setupGUI()
    }


    // Arguments have already been validated.
    void ingest(String basename, File inputFolder, File outputFolder, Closure startStep, Closure stopStep) {

        def workDir = new File(outputFolder, 'work')
        def modelFile = new File(dataDir, MODEL_FILENAME)

        // Create output folder now so we can create the log file.
        if (!outputFolder.exists()) {
            outputFolder.mkdirs()
        }

        // Capture stdout and stderr and write them to a log file in the output dir.
        def logFile = new File(outputFolder, basename + '.log')
        def logFilePrintStream = new PrintStream(logFile, "UTF-8")
        def lastLogFlushMillis = System.currentTimeMillis()

        setResult('')
        appendLineToResult("Logging detailed output below and to: ${logFile}.")

        // Clear any existing logging.
        logTextArea.setText('')

        final CHARS_TO_BUFFER = 1024
        final LOG_INTERVAL_MILLIS = 3000
        def logBuffer = new StringBuilder(CHARS_TO_BUFFER)
        def flushLogBuffer = {
            // Scrolling happens automatically due to DefaultCaret update policy.
            logTextArea.append(logBuffer.toString())
            logBuffer.setLength(0)
        }
            
        def writeToLogTextArea = { msg ->
            logBuffer.append(msg)
            // Flush if the buffer is big enough or it's been a while since the last flush.
            def currentTimeMillis = System.currentTimeMillis()
            if (logBuffer.length() > CHARS_TO_BUFFER || (currentTimeMillis - lastLogFlushMillis) > LOG_INTERVAL_MILLIS) {
                flushLogBuffer()
                lastLogFlushMillis = currentTimeMillis
            }
            return true;        // Also write to stdout/stderr
        }

        PrintStream stdout = System.out 
        PrintStream stderr = System.err

        System.setOut(logFilePrintStream)
        System.setErr(logFilePrintStream)

        def stdoutInterceptor = new SystemOutputInterceptor(writeToLogTextArea, true)
        def stderrInterceptor = new SystemOutputInterceptor(writeToLogTextArea, false)

        stdoutInterceptor.start()
        stderrInterceptor.start()

        def startTime = System.nanoTime()

        Ingester ingester = new Ingester()
        ingester.ingest(basename,
                        inputFolder,
                        outputFolder,
                        workDir,
                        dataDir,
                        modelFile,
                        startStep,
                        stopStep)

        flushLogBuffer()

        stdoutInterceptor.stop()
        stderrInterceptor.stop()

        System.setOut(stdout)
        System.setErr(stderr)

        logFilePrintStream.close()

        def elapsedSeconds = (System.nanoTime() - startTime) / 1.0e9
        
        appendLineToResult("Done. Results are in ${outputFolder}")
        appendLineToResult("Time: ${String.format("%.2f", elapsedSeconds)} seconds")

        // Oddly, this needs to be done twice. Maybe a repaint issue.
        flushLogBuffer()

    }


    void setupGUI() {
                
        swing = new SwingBuilder()
        final FOLDER_NAME_WIDTH = 40

        // Input folder UI
        def inputFolderTextField = swing.textField(columns: FOLDER_NAME_WIDTH)
        // Don't let it shrink to nothing.
        inputFolderTextField.setMinimumSize(inputFolderTextField.getPreferredSize());
        def chooseInputFolderButton = swing.button('Choose ...')
        inputFolderChooser = swing.fileChooser(fileSelectionMode: JFileChooser.DIRECTORIES_ONLY)
        chooseInputFolderButton.actionPerformed = {
            if (inputFolderChooser.showOpenDialog() != JFileChooser.APPROVE_OPTION) return
            inputFolderTextField.text = inputFolderChooser.selectedFile
        }

        // Output folder UI
        def outputFolderTextField = swing.textField(columns: FOLDER_NAME_WIDTH)
        outputFolderTextField.setMinimumSize(outputFolderTextField.getPreferredSize());
        def chooseOutputFolderButton = swing.button('Choose ...')
        outputFolderChooser = swing.fileChooser(fileSelectionMode: JFileChooser.DIRECTORIES_ONLY)
        chooseOutputFolderButton.actionPerformed = {
            if (outputFolderChooser.showOpenDialog() != JFileChooser.APPROVE_OPTION) return
            outputFolderTextField.text = outputFolderChooser.selectedFile
        }

        // Basename UI
        def basenameTextField = swing.textField(columns: FOLDER_NAME_WIDTH)
        basenameTextField.setMinimumSize(basenameTextField.getPreferredSize());

        def startIngestionButton = swing.button('Start Ingestion (will take minutes or hours)',
                                                alignmentX: JComponent.CENTER_ALIGNMENT)

        startIngestionButton.actionPerformed = {

            // Input folder validation.
            if (inputFolderTextField.text.isEmpty()) {
                JOptionPane.showMessageDialog(uiFrame, 'Please specify an input folder.')
                return
            }

            def inputFolder = new File(inputFolderTextField.text)
            if (!inputFolder.exists()) {
                JOptionPane.showMessageDialog(uiFrame, 'The input folder you specified does not exist.')
                return
            }
            if (!inputFolder.isDirectory()) {
                JOptionPane.showMessageDialog(uiFrame, 'The input pathname you specified is not a folder.')
                return
            }

            // Output folder validation
            if (outputFolderTextField.text.isEmpty()) {
                JOptionPane.showMessageDialog(uiFrame, 'Please specify an output folder.')
                return
            }

            def outputFolder = new File(outputFolderTextField.text)
            if (outputFolder.exists() && !outputFolder.isDirectory()) {
                JOptionPane.showMessageDialog(uiFrame, 'The output pathname you specified is not a folder.')
                return
            }

            // basename validation.
            if (basenameTextField.text.isEmpty()) {
                JOptionPane.showMessageDialog(uiFrame, 'Please specify an output files prefix.')
                return
            }
            
            if (basenameTextField.text =~ /[$DISALLOWED_BASENAME_CHARS]/) {
                JOptionPane.showMessageDialog(uiFrame, "Please do not use these characters in the output files prefix: $DISALLOWED_BASENAME_CHARS.")
                return
            }                
                
            swing.doOutside {
                try {
                    ingest(basenameTextField.text,
                           inputFolder,
                           outputFolder,
                           { step -> swing.edt { highlightStep(step)   } },
                           { step -> swing.edt { unhighlightStep(step) } })
                } catch (Throwable t) {
                    t.printStackTrace()
                    appendLineToResult("ERROR: Exception thrown. Check log file for details.\n")
                }
            }
        }
        
        uiFrame = swing.frame(title: 'VizLinc Document Ingester 1.0',
                              defaultCloseOperation:JFrame.EXIT_ON_CLOSE) {
            vbox {
                tableLayout(cellpadding: 10) {

                    tr {
                        td { label('<html>Input folder<br>(contains original documents:<br>.pdf, .txt, .doc, .docx, etc.)</html>') }
                        td { widget(inputFolderTextField) }
                        td { widget(chooseInputFolderButton) }
                    }

                    tr {
                        td { label('<html>Output folder<br>(.h2, .lucene, .graphml, + a work folder)</html>') }
                        td { widget(outputFolderTextField) }
                        td { widget(chooseOutputFolderButton) }
                    }

                    tr {
                        td { label('<html>Output files prefix</html>') }
                        td { widget(basenameTextField) }
                    }
                }
                
                vstrut(20)
                hbox {
                    hstrut(20)
                    vbox {
                        widget(startIngestionButton)
                        vstrut(20)
                        resultsTextArea = swing.textArea(
                            '',
                            alignmentX: JLabel.CENTER_ALIGNMENT,
                            rows: 4,
                            editable: false, cursor: null,
                            wrapStyleWord: true, lineWrap: false,
                            border: BorderFactory.createLoweredBevelBorder())
                        vstrut(20)
                        // We should just be able to return to exit, but Groovy 1.8.2 uses an ExecutorService for
                        // SwingBuilder.doOutside, which is never shutdown. So we have to force an exit. This is fixed in Groovy
                        // 2.1.  See https://jira.codehaus.org/browse/GROOVY-5074.
                        button('Exit', alignmentX: JLabel.CENTER_ALIGNMENT).actionPerformed = { swing.dispose() ; System.exit(0)  }
                    }
                    hstrut(width: 20)
                    vbox {
                        panel( border: BorderFactory.createTitledBorder('Ingestion Progress'), opaque: false) {
                            stepList = list(listData: Ingester.Step.values(), opaque: false)
                            // DOESN'T WORK; selections no longer get highlighted // stepList.getCellRenderer().setOpaque(false)
                            // Remove mouse listeners instead of disabling, so that the text color doesn't change.
                            stepList.getMouseListeners().each { stepList.removeMouseListener(it) }
                        }
                    }
                    hstrut(20)
                }
                vstrut(20)


                // large logging text area
                panel(border:  BorderFactory.createCompoundBorder(
                          BorderFactory.createEmptyBorder(10, 10, 10, 10),
                          BorderFactory.createTitledBorder('Detailed Ingestion Log'))) {
                    borderLayout()
                    logScrollPane = scrollPane(verticalScrollBarPolicy: ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS,
                                               horizontalScrollBarPolicy: ScrollPaneConstants.HORIZONTAL_SCROLLBAR_ALWAYS) {
                        logTextArea = textArea(
                            '',
                            alignmentX: JLabel.CENTER_ALIGNMENT,
                            rows: 24, columns: 80,
                            editable: false,
                            cursor: null)
                        // Scroll to bottom automatically.
                        logTextArea.getCaret().setUpdatePolicy(DefaultCaret.ALWAYS_UPDATE)
                    }
                }
            }
        }
        
        uiFrame.pack()
        uiFrame.show()
    }


    def appendLineToResult(text) {
        swing.edt {
            resultsTextArea.append(text)
            resultsTextArea.append('\n')
        }
    }

    def setResult(text) {
        swing.edt {
            resultsTextArea.setText(text)
        }
    }

    def highlightStep(step) {
        stepList.setSelectedValue(step, true /* should scroll */)
    }

    def unhighlightStep(step) {
        stepList.clearSelection()
    }

}
