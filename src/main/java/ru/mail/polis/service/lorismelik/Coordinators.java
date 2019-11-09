package ru.mail.polis.service.lorismelik;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;

import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.http.HttpException;
import one.nio.http.HttpSession;
import one.nio.pool.PoolException;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.lorismelik.RocksDAO;
import ru.mail.polis.dao.TimestampRecord;

import java.io.IOException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.net.http.HttpResponse.BodyHandlers.ofByteArray;
import static java.net.http.HttpResponse.BodyHandlers.ofString;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.stream.Collectors.toList;

public class Coordinators {
    private final RocksDAO dao;
    private final NodeDescriptor nodes;
    private static final Logger logger = Logger.getLogger(Coordinators.class.getName());
    private static final HttpClient client = HttpClient.newHttpClient();
    private static final String PROXY_HEADER = "X-OK-Proxy: True";
    private static final String ENTITY_HEADER = "/v0/entity?id=";

    /**
     * Create the cluster coordinator instance.
     *
     * @param nodes          to specify cluster nodes
     * @param dao            to specify current DAO
     */
    public Coordinators(@NotNull final NodeDescriptor nodes,
                        @NotNull final DAO dao) {
        this.dao = (RocksDAO) dao;
        this.nodes = nodes;
    }

    /**
     * Coordinate the delete among all clusters.
     *
     * @param replicaNodes to define the nodes where to create replicas
     * @param rqst         to define request
     * @param acks         to specify the amount of acks needed
     */
    public void coordinateDelete(final String[] replicaNodes,
                                     @NotNull final HttpSession session,
                                     @NotNull final Request rqst,
                                     final int acks) {
        final String id = rqst.getParameter("id=");
        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final AtomicInteger asks = new AtomicInteger(0);
        final Function<HttpRequest.Builder, HttpRequest.Builder> methodDefiner = HttpRequest.Builder::DELETE;
        final List<String> uris = Arrays.asList(replicaNodes);
        CompletableFuture local = CompletableFuture.runAsync(() -> {
            if (uris.contains(nodes.getId())) {
                try {
                    deleteWithTimestampMethodWrapper(key);
                    asks.incrementAndGet();
                } catch (IOException e) {
                    logger.log(Level.SEVERE, "Exception while deleting by proxy: ", e);
                }
            }
        });
        List<HttpRequest> requests = createRequests(uris, id, methodDefiner);
        List<CompletableFuture> futureList = requests.stream()
                .map(request -> client.sendAsync(request, ofByteArray())
                        .thenAccept(response -> {
                            if (response.statusCode() == 202)
                                asks.incrementAndGet();
                            if(asks.getPlain() >= acks)
                                try {
                                    session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
                                } catch (IOException e) {
                                    logger.log(Level.SEVERE, "Exception while deleting by proxy: ", e);
                                }
                        }))
                .collect(Collectors.toList());
        futureList.add(local);
        processError.accept(session, futureList);
    }

    /**
     * Coordinate the put among all clusters.
     *
     * @param replicaNodes to define the nodes where to create replicas
     * @param rqst         to define request
     * @param acks         to specify the amount of acks needed
     */
    public void coordinatePut(final String[] replicaNodes,
                                  @NotNull final HttpSession session,
                                  @NotNull final Request rqst,
                                  final int acks) throws IOException {
        final String id = rqst.getParameter("id=");
        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final AtomicInteger asks = new AtomicInteger(0);
        Function<HttpRequest.Builder, HttpRequest.Builder> methodDefiner =
                x -> x.PUT(HttpRequest.BodyPublishers.ofByteArray(rqst.getBody()));
        List<String> uris = Arrays.asList(replicaNodes);
        CompletableFuture local = CompletableFuture.runAsync(() -> {
            if (uris.contains(nodes.getId())) {
                try {
                    putWithTimestampMethodWrapper(key, rqst);
                    asks.incrementAndGet();
                } catch (IOException e) {
                    logger.log(Level.SEVERE, "Exception while deleting by proxy: ", e);
                }
            }
        });
        List<HttpRequest> requests = createRequests(uris, id, methodDefiner);
        List<CompletableFuture> futureList = requests.stream()
                .map(request -> client.sendAsync(request, ofByteArray())
                        .thenAccept(response -> {
                            if (response.statusCode() == 201)
                                asks.incrementAndGet();
                            if(asks.getPlain() >= acks)
                                try {
                                    session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
                                } catch (IOException e) {
                                    logger.log(Level.SEVERE, "Exception while deleting by proxy: ", e);
                                }
                        }))
                .collect(Collectors.toList());
        futureList.add(local);
        processError.accept(session, futureList);
    }

    /**
     * Coordinate the get among all clusters.
     *
     * @param replicaNodes to define the nodes where to create replicas
     * @param rqst         to define request
     * @param acks         to specify the amount of acks needed
     * @return Response value
     */
    public Response coordinateGet(final String[] replicaNodes,
                                  @NotNull final Request rqst,
                                  final int acks) throws IOException {
        final String id = rqst.getParameter("id=");
        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        int asks = 0;
        final List<TimestampRecord> responses = new ArrayList<>();
        for (final String node : replicaNodes) {
            try {
                Response respGet;
                if (node.equals(nodes.getId())) {
                    respGet = getWithTimestampMethodWrapper(key);
                } else {
                    respGet = clusterClients.get(node)
                            .get(ENTITY_HEADER + id, PROXY_HEADER);
                }
                if (respGet.getStatus() == 404 && respGet.getBody().length == 0) {
                    responses.add(TimestampRecord.getEmpty());
                } else if (respGet.getStatus() == 500) {
                    continue;
                } else {
                    responses.add(TimestampRecord.fromBytes(respGet.getBody()));
                }
                asks++;
            } catch (HttpException | PoolException | InterruptedException e) {
                logger.log(Level.SEVERE, "Exception while putting!", e);
            }
        }
        final boolean proxied = rqst.getHeader(PROXY_HEADER) != null;
        if (asks >= acks || proxied) {
            return processResponses(replicaNodes, responses, proxied);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response processResponses(final String[] replicaNodes,
                                      final List<TimestampRecord> responses,
                                      final boolean proxied) throws IOException {
        final TimestampRecord mergedResp = TimestampRecord.merge(responses);
        if (mergedResp.isValue()) {
            if (!proxied && replicaNodes.length == 1) {
                return new Response(Response.OK, mergedResp.getValueAsBytes());
            } else if (proxied && replicaNodes.length == 1) {
                return new Response(Response.OK, mergedResp.toBytes());
            } else {
                return new Response(Response.OK, mergedResp.getValueAsBytes());
            }
        } else if (mergedResp.isDeleted()) {
            return new Response(Response.NOT_FOUND, mergedResp.toBytes());
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private void putWithTimestampMethodWrapper(final ByteBuffer key, final Request request) throws IOException {
        dao.upsertRecordWithTimestamp(key, ByteBuffer.wrap(request.getBody()));
    }

    private void deleteWithTimestampMethodWrapper(final ByteBuffer key) throws IOException {
        dao.removeRecordWithTimestamp(key);
    }

    @NotNull
    private Response getWithTimestampMethodWrapper(final ByteBuffer key) throws IOException {
        try {
            final byte[] res = copyAndExtractWithTimestampFromByteBuffer(key);
            return new Response(Response.OK, res);
        } catch (NoSuchElementException exp) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private byte[] copyAndExtractWithTimestampFromByteBuffer(@NotNull final ByteBuffer key) throws IOException {
        final TimestampRecord res = dao.getRecordWithTimestamp(key);
        if (res.isEmpty()) {
            throw new NoSuchElementException("Element not found!");
        }
        return res.toBytes();
    }

    /**
     * Coordinate the request among all clusters.
     *
     * @param replicaClusters to define the nodes where to create replicas
     * @param request         to define request
     * @param acks            to specify the amount of acks needed
     * @param session         to specify the session where to output messages
     */
    public void coordinateRequest(final String[] replicaClusters,
                                  final Request request,
                                  final int acks,
                                  final HttpSession session) throws IOException {
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    session.sendResponse(coordinateGet(replicaClusters, request, acks));
                    break;
                case Request.METHOD_PUT:
                    coordinatePut(replicaClusters, session, request, acks);
                    break;
                case Request.METHOD_DELETE:
                    coordinateDelete(replicaClusters, session, request, acks);
                    break;
                default:
                    session.sendError(Response.METHOD_NOT_ALLOWED, "Wrong method");
                    break;
            }
        } catch (IOException e) {
            session.sendError(Response.GATEWAY_TIMEOUT, e.getMessage());
        }
    }

    private List<HttpRequest> createRequests(List<String> uris,
                                             String id,
                                             Function<HttpRequest.Builder, HttpRequest.Builder> methodDefiner) {
        return  uris.stream()
                .map(x -> x + ENTITY_HEADER + id)
                .map(wrapper(URI::new))
                .map(HttpRequest::newBuilder)
                .map(x -> x.setHeader("X-OK-Proxy", "true"))
                .map(methodDefiner)
                .map(x -> x.timeout(Duration.of(3, SECONDS)))
                .map(HttpRequest.Builder::build)
                .collect(toList());
    }

    private final BiConsumer<HttpSession, List<CompletableFuture>> processError = ((session, futureList) ->
            CompletableFuture.allOf(futureList.toArray(CompletableFuture<?>[]::new))
            .exceptionally(x -> {
                try {
                    session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                } catch (IOException e) {
                    logger.log(Level.SEVERE, "Exception while deleting by proxy: ", e);
                }
                return null;
            })
            .thenAccept(x -> {
                try {
                    session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                } catch (IOException e) {
                    logger.log(Level.SEVERE, "Exception while deleting by proxy: ", e);
                }
            }));

    // Its a wrapper to catch exceptions in stream
    @FunctionalInterface
    public interface FunctionWithException<T, R, E extends Exception> {
        R apply(T t) throws E;
    }

    private <T, R, E extends Exception>
    Function<T, R> wrapper(FunctionWithException<T, R, E> fe) {
        return arg -> {
            try {
                return fe.apply(arg);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }
}
