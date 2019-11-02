package ru.mail.polis.service.lorismelik;

import com.google.common.base.Splitter;
import one.nio.http.HttpSession;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.List;

import static one.nio.http.Response.BAD_REQUEST;

public class ReplicaFactor {
    private final int ack;
    private final int from;

    public ReplicaFactor(final int ack, final int from) {
        this.ack = ack;
        this.from = from;
    }

    @NotNull
    private static ReplicaFactor of(@NotNull final String value) {
        final String rem = value.replace("=", "");
        final List<String> values = Splitter.on('/').splitToList(rem);
        if (values.size() != 2) {
            throw new IllegalArgumentException("Wrong replica factor:" + value);
        }
        return new ReplicaFactor(Integer.parseInt(values.get(0)), Integer.parseInt(values.get(1)));
    }

    /**
     * Calculate the ReplicaFactor value.
     *
     * @param replicas    to define the amount of replicas needed
     * @param session     to output responses
     * @param defaultReplicaFactor   to specify the default ReplicaFactor
     * @param clusterSize to specify the size of cluster
     * @return RF value
     */
    public static ReplicaFactor calculateRF(final String replicas, @NotNull final HttpSession session,
                                            final ReplicaFactor defaultReplicaFactor, final int clusterSize) throws IOException {
        ReplicaFactor replicaFactor = null;
        try {
            replicaFactor = replicas == null ? defaultReplicaFactor : ReplicaFactor.of(replicas);
            if (replicaFactor.ack < 1 || replicaFactor.from < replicaFactor.ack || replicaFactor.from > clusterSize) {
                throw new IllegalArgumentException("From is too big!");
            }
            return replicaFactor;
        } catch (IllegalArgumentException e) {
            session.sendError(BAD_REQUEST, "Wrong ReplicaFactor!");
        }
        return replicaFactor;
    }

    public int getFrom() {
        return from;
    }

    public int getAck() {
        return ack;
    }
}
