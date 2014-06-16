import com.tinkerpop.blueprints.*

class NodeAndText {
    Vertex node
    String text

    NodeAndText(Vertex node, String text) {
        this.node = node
        this.text = text
    }

    String toString() {
        return "(node=${node}, text=$text)"
    }

}

