import static edu.mit.ll.vizlincdb.util.VizLincProperties.*
import edu.mit.ll.vizlincdb.graph.VizLincDB
import java.text.Normalizer
import java.text.Normalizer.Form
import com.wcohen.ss.*

abstract class CorefBase {

    // Fields
    VizLincDB db

    CorefBase(graphdb) {
        db = graphdb
    }

    def addEntitiesToGraph (clusters, entity_type, created_by) {
        for (j in clusters) {
            def name = j.key
            def mentions = j.value
            // println "name: $name, mentions: $mentions"
            def node = db.newEntity(entity_type, name, created_by)
            node.setProperty('num_mentions',mentions.size())
            node.setProperty('num_docs', 1)
            db.connectEntityToMentionsAndDocuments(node, mentions.collect{it.node})
        }
        db.commit()
    }

    def addEntitiesToGraphByGlobalID (clusters, entity_type, created_by) {
        for (j in clusters) {
            def gid = j.key
            def mentions = j.value
            def name = mentions[0].text // All the same
            // println "name: $name, mentions: $mentions"
            def node = db.newEntity(entity_type, name, created_by)
            node.setProperty(P_ENTITY_GLOBAL_ID, gid)
            node.setProperty('num_mentions',mentions.size())
            node.setProperty('num_docs', 1)
            db.connectEntityToMentionsAndDocuments(node, mentions.collect{it.node})
        }
        db.commit()
    }

    final static JUNK_CHARS_TO_REMOVE = /[\^\"\<\>_]/
    // " is a good char in a loc
    final static JUNK_CHARS_TO_REMOVE_FOR_LOCS = /[\^\<\>_]/

    // Return a list of NodeAndText objects. Each NodeAndText object references a node and also extracts the current value of
    // the P_MENTION_TEXT property. The texts are extracted so that they may be modified later.
    // The mention texts are normalized and then the list is sorted by the text. Mention texts that become empty after
    // normalization are dropped.
    def getNormalizedMentions(doc_node, node_type, junkToRemove = JUNK_CHARS_TO_REMOVE) {
        return db.getMentionsInDocumentOfType(doc_node, node_type).toList().collect { node ->
            def text = normalizeStr(node.getProperty(P_MENTION_TEXT), junkToRemove)
            return text ? new NodeAndText(node, text) : null
        }.grep{it}.sort{it.text}
    }

    // As above, but without normalization (assume they are already normalized)
    def getTwitterNormalizedMentions(doc_node, node_type) {
        return db.getMentionsInDocumentOfType(doc_node, node_type).toList().collect { node ->
            def text = twitterNormalizeStr(node.getProperty(P_MENTION_TEXT))
            return text ? new NodeAndText(node, text) : null
        }.grep{it}.sort{it.text}
    }

    // As above, but without normalization (assume they are already normalized)
    def getMentions(doc_node, node_type) {
        return db.getMentionsInDocumentOfType(doc_node, node_type).toList().collect { node ->
            def text = node.getProperty(P_MENTION_TEXT)
            return text ? new NodeAndText(node, text) : null
        }.grep{it}.sort{it.text}
    }

    def normalizeStr (String str, junkToRemove) {
        def NON_WORD = '[^A-Za-z0-9-]'
        def out = str.trim().toUpperCase()
        // Remove accents.
        out = Normalizer.normalize(out, Form.NFD).replaceAll("\\p{InCombiningDiacriticalMarks}+","")
        // Normalize whitespace to single spaces.
        out = out.replaceAll(/[\s]+/, " ")
        // Remove leading and trailing non-word chars.
        out = out.replaceAll("^$NON_WORD+", '').replaceAll("$NON_WORD+\$", '')
        // Replace junk chars
        out = out.replaceAll(junkToRemove, " ")
        return out
    }

    def twitterNormalizeStr (String str) {
        def out = str.trim().toLowerCase()
        out = Normalizer.normalize(out, Form.NFD).replaceAll("\\p{InCombiningDiacriticalMarks}+","")
        return out
    }

    def combineFirstnameFullname (clusters) {

        def min_num_chars = 2

        // First pass -- find candidate first names
        // Look for <first name> followed by <first name> <rest>, where "first name" is loosely interpreted
        def first_names = [:]
        def first_names_full = [:]
        def last = ""
        def curr_first_name = null
        for (j in clusters) {
            def curr = j.key
            def min_len = [curr.length(),last.length()].min()
            if (last != "" && last == curr.substring(0,min_len) && min_len>=min_num_chars) { // first names at least 2 characters
                curr_first_name = last
                first_names[last] = 1
                first_names_full[last] = curr
            } else if (curr_first_name != null){  // check to see how many subsequent names have this "first name"
                min_len = [curr.length(),curr_first_name.length()].min()
                if (curr.substring(0,min_len)==curr_first_name) {
                    first_names[curr_first_name] += 1
                }
            }
            last = j.key
        }

        // Second pass -- if there's exactly one match, then combine.  Otherwise, this first name may appear multiple times.
        def num_combined = 0
        for (fn_map in first_names) {
            def fn = fn_map.key
            def fn_full = first_names_full[fn]
            if (fn_map.value == 1) {
                clusters[fn_full] = clusters[fn_full] + clusters[fn]
                clusters.remove(fn)
                num_combined += 1
            }
        }

    }

    def combineByExactMatch (in_list) {
        // Take a name list and produce the first set of clusters using exact string match
        def out_map = [:]  // Default hash map in groovy is linked, so order is maintained
        def last = null
        def curr_list = null
        for (it in in_list) {
            if (it.text==last) {
                curr_list.add(it)
            } else {
                if (curr_list != null) {
                    out_map[last] = curr_list
                }
                curr_list = [it]
            }
            last = it.text
        }
        if (curr_list!=null) {
            out_map[last] = curr_list
        }
        return out_map
    }

    def combineByGlobalID (in_list) {
        // Take a name list and produce the first set of clusters using only global IDs
        def out_map = [:]  // Default hash map in groovy is linked, so order is maintained

        for (it in in_list) {
            def gid = it.node[P_MENTION_GLOBAL_ID] 
            if (gid) {
                if (!out_map.containsKey(gid)) 
                    out_map[gid] = []
                out_map[gid].add(it)
            }
        }
        return out_map
    }

    static final Levenstein LEVENSTEIN = new Levenstein()
    // def static LEVEL2JAROWINKLER = new Level2JaroWinkler()
    // def static LEVEL2JAROWINKLER_THRESH = 0.9

    def combineAdjacentClose (clusters, dist=LEVENSTEIN, thresh=-1.1, match_numbers_exactly=true) {
        // Combine two names if they are close both in edit distance and in a sorted list
        // First character OCR errors are a problem

        def candidate_merge = [:]
        def last = ""

        // Find merge candidates
        for (j in clusters) {
            def curr = j.key
            def d = dist.score(last, curr)
            if (last!="" && dist.score(last, curr) > thresh && (!match_numbers_exactly || allNumbersMatch(last, curr))) {
                candidate_merge[curr] = last
                // Uncomment to check merge criteria.
                // println "yesmerge: $d\t$last ::: $curr"  // XXX
            } else {
                // Uncomment to check merge criteria.
                // println "nomerge: $d\t$last ::: $curr"  // XXX
            }
            last = curr
        }

        // Now merge
        for (candidate in candidate_merge) {
            def nm1 = candidate.key
            def nm2 = candidate.value
            if (clusters.containsKey(nm1) && clusters.containsKey(nm2)) {
                if (clusters[nm2].size() > clusters[nm1].size()) { // Pick the cluster with the most elements
                    clusters[nm2] = clusters[nm2] + clusters[nm1]
                    clusters.remove(nm1)
                } else {
                    clusters[nm1] = clusters[nm1] + clusters[nm2]
                    clusters.remove(nm2)
                }
            }
        }
    }

    def static NUMBER_PATTERN = ~ /\d+/
    // Make sure any sequences of digits in s and t match exactly.
    def allNumbersMatch(s, t) {
        return ((s =~ NUMBER_PATTERN).collect{it}) == ((t =~ NUMBER_PATTERN).collect{it})
    }

    def mergeClusters (clusters, created_by) {
        def i = 0
        println "Edges before mergeClusters: ${db.graph.E.count()}"
        for (c in clusters) {
            if (((i%100) == 0) || (c.value.size() > 100)) {
                println "Merging cluster: " + i + " with size " + c.value.size()
            }

            // Find most common name -- largest subcluster
            def names = [:]
            c.value.each{ names[it.text]=0 }
            c.value.each{ names[it.text] += 1 }
            def most_common_name = names.max{ it.value }.key
            def most_common_node = c.value.find{ it.text==most_common_name }.node
            most_common_node.setProperty(P_CREATED_BY, created_by)

            // Loop over nodes in cluster and merge

            def num = 0
            for (pr in c.value) {
                def node = pr.node
                if (node!=most_common_node) {
                    // Redo edges to most common node and delete
                    most_common_node.setProperty('num_mentions',
                                                 most_common_node.getProperty('num_mentions') +
                                                 node.getProperty('num_mentions'))
                    most_common_node.setProperty('num_docs', most_common_node.getProperty('num_docs') + 1)
                    // .toList() to get the whole list because otherwise the
                    // connectEntityToMentionsAndDocuments() will add new edges
                    // that will be picked up by the Gremlin pipeline while it is still
                    // iterating.
                    def mention_nodes = node.inE(L_MENTION_TO_ENTITY).outV.toList()
                    db.connectEntityToMentionsAndDocuments(most_common_node, mention_nodes)
                    db.deleteNode(node)
                    num += 1
                    if (num > 1000) {
                        db.commit()
                        num = 0
                    }
                }
            }

            if (((i%100) == 0) || (c.value.size() > 50)) {
                db.commit()
            }

            i += 1
        }
        db.commit()
        println "Edges after mergeClusters: ${db.graph.E.count()}"
    }

    def outputClusters (clusters) {
        println "Clusters are (" + clusters.size() + "):"
        for (j in clusters) {
            println j.key  + " " + j.value.size() + " " + j.value
        }
    }

}