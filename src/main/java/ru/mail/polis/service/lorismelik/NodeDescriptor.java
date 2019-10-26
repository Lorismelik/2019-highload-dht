package ru.mail.polis.service.lorismelik;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NodeDescriptor {

    private final List<String> nodes;
    private final String id;

    public NodeDescriptor(@NotNull final Set<String> nodes, @NotNull final String id) {
        this.nodes = new ArrayList<>(nodes);
        this.id = id;
    }

    /**
     * Define node by key.
     *
     * @param key key to found
     * @return id of the cluster node
     */
    public String getNodeIdByKey(@NotNull final ByteBuffer key) {
        return nodes.get((key.hashCode() & Integer.MAX_VALUE) % nodes.size());
    }

    public Set<String> getNodes() {
        return new HashSet<>(this.nodes);
    }

    public String getId() {
        return this.id;
    }
}
