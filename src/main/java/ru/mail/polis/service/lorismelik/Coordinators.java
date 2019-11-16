package ru.mail.polis.service.lorismelik;

import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.Session;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.TimestampRecord;
import ru.mail.polis.dao.lorismelik.RocksDAO;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.net.http.HttpResponse.BodyHandlers.ofByteArray;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.logging.Level.SEVERE;
import static java.util.stream.Collectors.toList;

class Coordinators {
    private final RocksDAO dao;
    private final NodeDescriptor nodes;
    private static final Logger logger = Logger.getLogger(Coordinators.class.getName());
    private static final HttpClient client = HttpClient.newHttpClient();
    private static final String PROXY_HEADER = "X-OK-Proxy: True";
    private static final long  KEEP_ALIVE = 5000;

    /**
     * Create the cluster coordinator instance.
     *
     * @param nodes to specify cluster nodes
     * @param dao   to specify current DAO
     */
    Coordinators(@NotNull final NodeDescriptor nodes,
                 @NotNull final DAO dao) {
        this.dao = (RocksDAO) dao;
        this.nodes = nodes;
    }

    /**
     * Coordinate the delete among all clusters.
     *
     * @param replicaNodes to define the nodes where to create replicas
     * @param session      http session to send response
     * @param rqst         to define request
     * @param acks         to specify the amount of acks needed
     */
    private  void coordinateDelete(final String[] replicaNodes,
                                  @NotNull final HttpSession session,
                                  @NotNull final Request rqst,
                                  final int acks) {
        final String id = rqst.getParameter("id=");
        final boolean proxied = rqst.getHeader(PROXY_HEADER) != null;
        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final AtomicInteger asks = new AtomicInteger(0);
        final Function<HttpRequest.Builder, HttpRequest.Builder> methodDefiner = HttpRequest.Builder::DELETE;
        Consumer<Void> returnResult = x -> {
            if ((asks.getAcquire() >= acks || proxied) && checkConnection(session))
                try {
                    session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
                } catch (IOException e) {
                    session.close();
                }
        };
        ArrayList<String> uris = new ArrayList<>(Arrays.asList(replicaNodes));
        CompletableFuture<Void> local = null;
        if (uris.remove(nodes.getId())) {
            local = CompletableFuture.runAsync(() -> {
                try {
                    deleteWithTimestampMethodWrapper(key);
                    asks.incrementAndGet();
                } catch (IOException e) {
                    logger.log(SEVERE, "Exception while delete", e);
                }

            }).thenAccept(returnResult);
        }
        List<HttpRequest> requests = createRequests(uris, rqst, methodDefiner);
        List<CompletableFuture<Void>> futureList = requests.stream()
                .map(request -> client.sendAsync(request, ofByteArray())
                        .thenAccept(response -> {
                            if (response.statusCode() == 202)
                                asks.incrementAndGet();
                            returnResult.accept(null);
                        }))
                .collect(Collectors.toList());
        if (local != null) {
            futureList.add(local);
        }
        processError.accept(session, futureList, acks, asks, proxied);
    }

    /**
     * Coordinate the put among all clusters.
     *
     * @param replicaNodes to define the nodes where to create replicas
     * @param session      http session to send response
     * @param rqst         to define request
     * @param acks         to specify the amount of acks needed
     */
    private void coordinatePut(final String[] replicaNodes,
                               @NotNull final HttpSession session,
                               @NotNull final Request rqst,
                               final int acks) throws IOException {
        final String id = rqst.getParameter("id=");
        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final AtomicInteger asks = new AtomicInteger(0);
        final boolean proxied = rqst.getHeader(PROXY_HEADER) != null;
        Function<HttpRequest.Builder, HttpRequest.Builder> methodDefiner =
                x -> x.PUT(HttpRequest.BodyPublishers.ofByteArray(rqst.getBody()));
        Consumer<Void> returnResult = x -> {
            if ((asks.getAcquire() >= acks || proxied) && checkConnection(session))
                try {
                    session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
                } catch (IOException e) {
                    session.close();
                }
        };
        ArrayList<String> uris = new ArrayList<>(Arrays.asList(replicaNodes));
        CompletableFuture<Void> local = null;
        if (uris.remove(nodes.getId())) {
            local = CompletableFuture.runAsync(() -> {
                try {
                    putWithTimestampMethodWrapper(key, rqst);
                    asks.incrementAndGet();
                } catch (IOException e) {
                    logger.log(SEVERE, "Exception while put ", e);
                }
            }).thenAccept(returnResult);
        }
        List<HttpRequest> requests = createRequests(uris, rqst, methodDefiner);
        List<CompletableFuture<Void>> futureList = requests.stream()
                .map(request -> client.sendAsync(request, ofByteArray())
                        .thenAccept(response -> {
                            if (response.statusCode() == 201)
                                asks.incrementAndGet();
                            returnResult.accept(null);
                        }))
                .collect(Collectors.toList());
        if (local != null) {
            futureList.add(local);
        }
        processError.accept(session, futureList, acks, asks, proxied);
    }

    /**
     * Coordinate the get among all clusters.
     *
     * @param replicaNodes to define the nodes where to create replicas
     * @param session      http session to send response
     * @param rqst         to define request
     * @param acks         to specify the amount of acks needed
     */
    private void coordinateGet(final String[] replicaNodes,
                               @NotNull final HttpSession session,
                               @NotNull final Request rqst,
                               final int acks) throws IOException {
        final String id = rqst.getParameter("id=");
        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final AtomicInteger asks = new AtomicInteger(0);
        final boolean proxied = rqst.getHeader(PROXY_HEADER) != null;
        final Function<HttpRequest.Builder, HttpRequest.Builder> methodDefiner = HttpRequest.Builder::GET;
        ArrayList<String> uris = new ArrayList<>(Arrays.asList(replicaNodes));
        final List<TimestampRecord> responses = Collections.synchronizedList(new ArrayList<>());
        Consumer<Void> returnResult = x -> {
            if ((asks.getAcquire() >= acks || proxied) && checkConnection(session)) {
                try {
                    session.sendResponse(processResponses(replicaNodes, responses, proxied));
                } catch (IOException e) {
                    session.close();
                }
            }
        };
        CompletableFuture<Void> local = null;
        if (uris.remove(nodes.getId())) {
            local = CompletableFuture.runAsync(() -> {
                try {
                    final Response localResponse = getWithTimestampMethodWrapper(key);
                    if (localResponse.getBody().length == 0)
                        responses.add(TimestampRecord.getEmpty());
                    else
                        responses.add(TimestampRecord.fromBytes(localResponse.getBody()));
                    asks.incrementAndGet();
                } catch (IOException e) {
                    logger.log(SEVERE, "Exception while deleting by proxy: ", e);
                }

            }).thenAccept(returnResult);
        }

        List<HttpRequest> requests = createRequests(uris, rqst, methodDefiner);
        List<CompletableFuture<Void>> futureList = requests.stream()
                .map(request -> client.sendAsync(request, ofByteArray())
                        .thenAccept(response -> {
                            if (response.statusCode() == 404 && response.body().length == 0) {
                                responses.add(TimestampRecord.getEmpty());
                            }
                            if (response.statusCode() == 500) return;
                            responses.add(TimestampRecord.fromBytes(response.body()));
                            asks.incrementAndGet();
                            returnResult.accept(null);
                        }))
                .collect(Collectors.toList());
        if (local != null) {
            futureList.add(local);
        }
        processError.accept(session, futureList, acks, asks, proxied);
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

    private boolean checkConnection(@NotNull final HttpSession session) {
        return session.checkStatus(System.currentTimeMillis(), KEEP_ALIVE) == Session.ACTIVE;
    }
    /**
     * Coordinate the request among all clusters.
     *
     * @param replicaClusters to define the nodes where to create replicas
     * @param request         to define request
     * @param acks            to specify the amount of acks needed
     * @param session         to specify the session where to output messages
     */
    void coordinateRequest(final String[] replicaClusters,
                           final Request request,
                           final int acks,
                           final HttpSession session) throws IOException {
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    coordinateGet(replicaClusters, session, request, acks);
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

    /**
     * Create list of requests which will be send to other nodes
     *
     * @param uris    addresses of other nodes
     * @param rqst    http request which was accepted from client
     * @param methodDefiner define http method of requests
     */
    private List<HttpRequest> createRequests(List<String> uris,
                                             Request rqst,
                                             Function<HttpRequest.Builder, HttpRequest.Builder> methodDefiner)  {
        return uris.stream()
                .map(x -> x + rqst.getURI())
                .map(wrapper(URI::new))
                .map(HttpRequest::newBuilder)
                .map(x -> x.setHeader("X-OK-Proxy", "true"))
                .map(methodDefiner)
                .map(x -> x.timeout(Duration.of(5, SECONDS)))
                .map(HttpRequest.Builder::build)
                .collect(toList());
    }

    /**
     * Wrapper to avoid try/catch exceptions in a stream
     *
     * @param fe function in a stream, which can throw exception
     */
    private final MyConsumer<HttpSession,
            List<CompletableFuture<Void>>, Integer, AtomicInteger, Boolean> processError =
            ((session, futureList, neededAcks, receivedAcks, proxied) ->
            CompletableFuture.allOf(futureList.toArray(CompletableFuture<?>[]::new))
                    .thenAccept(x -> {
                        if (receivedAcks.getAcquire() < neededAcks && !(proxied && receivedAcks.getAcquire() == 1) && checkConnection(session))
                            try {
                                session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                            } catch (IOException e) {
                                session.close();
                            }
                    })
                    .exceptionally(x -> {
                        if (receivedAcks.getAcquire() < neededAcks && !(proxied && receivedAcks.getAcquire() == 1) && checkConnection(session))
                            try {
                                session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                            } catch (IOException e) {
                                session.close();
                            }
                        return null;
                    }));


    @FunctionalInterface
    public interface FunctionWithException<T, R, E extends Exception> {
        R apply(T t) throws E;
    }

    /**
     * Wrapper to avoid try/catch exceptions in a stream
     *
     * @param fe function in a stream, which can throw exception
     */
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

    @FunctionalInterface
    public interface MyConsumer<T, U, R, Y, C> {
        void accept(T t, U u, R r, Y y, C c);
    }
}
