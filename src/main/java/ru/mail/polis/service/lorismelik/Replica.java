package ru.mail.polis.service.lorismelik;

import com.google.common.base.Splitter;
import one.nio.http.HttpSession;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.List;

import static one.nio.http.Response.BAD_REQUEST;

public class Replica {
    private final int ack;
    private final int from;

    public Replica(final int ack, final int from) {
        this.ack = ack;
        this.from = from;
    }

    @NotNull
    private static Replica of(@NotNull final String value) {
        final String rem = value.replace("=", "");
        final List<String> values = Splitter.on('/').splitToList(rem);
        if (values.size() != 2) {
            throw new IllegalArgumentException("Wrong replica factor:" + value);
        }
        return new Replica(Integer.parseInt(values.get(0)), Integer.parseInt(values.get(1)));
    }

    /**
     * Calculate the RF value.
     *
     * @param replicas    to define the amount of replicas needed
     * @param session     to output responses
     * @param defaultReplica   to specify the default RF
     * @param clusterSize to specify the size of cluster
     * @return RF value
     */
    public static Replica calculateRF(final String replicas, @NotNull final HttpSession session,
                                      final Replica defaultReplica, final int clusterSize) throws IOException {
        Replica replica = null;
        try {
            replica = replicas == null ? defaultReplica : Replica.of(replicas);
            if (replica.ack < 1 || replica.from < replica.ack || replica.from > clusterSize) {
                throw new IllegalArgumentException("From is too big!");
            }
            return replica;
        } catch (IllegalArgumentException e) {
            session.sendError(BAD_REQUEST, "Wrong RF!");
        }
        return replica;
    }

    public int getFrom() {
        return from;
    }

    public int getAck() {
        return ack;
    }
}
