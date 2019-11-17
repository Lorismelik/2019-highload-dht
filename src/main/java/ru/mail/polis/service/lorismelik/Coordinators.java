package ru.mail.polis.service.lorismelik;

import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Utils;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.TimestampRecord;
import ru.mail.polis.dao.lorismelik.RocksDAO;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.net.http.HttpResponse.BodyHandlers.ofByteArray;

class Coordinators {
    private final RocksDAO dao;
    private final NodeDescriptor nodes;
    private static final HttpClient client = HttpClient.newHttpClient();
    private static final String PROXY_HEADER = "X-OK-Proxy: True";

    private final Utils.MyConsumer<HttpSession,
            List<CompletableFuture<Void>>,
            Integer,
            AtomicInteger,
            Boolean> processError = (session, futureList, neededAcks, receivedAcks, proxied) -> {
        if (receivedAcks.getAcquire() < neededAcks
                && !(proxied && receivedAcks.getAcquire() == 1))
            try {
                session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
            } catch (IOException e) {
                session.close();
            }
    };

    private void checkResponses(final List<CompletableFuture<Void>> futureList,
                                final HttpSession session,
                                final Integer neededAcks,
                                final AtomicInteger receivedAcks,
                                final Boolean proxied) {
        CompletableFuture.allOf(futureList.toArray(CompletableFuture<?>[]::new))
                .thenAccept(x -> processError.accept(session, futureList, neededAcks, receivedAcks, proxied))
                .exceptionally(x -> {
                            processError.accept(session, futureList, neededAcks, receivedAcks, proxied);
                            return null;
                        }
                );
    }

    private void processPutAndDeleteRequest(final List<HttpRequest> requests,
                                            final AtomicInteger receivedAcks,
                                            final Supplier<Response> successResponse,
                                            final HttpSession session,
                                            final Integer neededAcks) {

        final boolean proxied = requests.isEmpty();
            final List<CompletableFuture<Void>> futureList = requests.stream()
                    .map(request -> client.sendAsync(request, ofByteArray())
                            .thenAccept(response -> {
                                if (response.statusCode() == successResponse.get().getStatus())
                                    receivedAcks.incrementAndGet();
                                sendResult(successResponse, neededAcks, receivedAcks, session, false);
                            }))
                    .collect(Collectors.toList());
            checkResponses(futureList, session, neededAcks, receivedAcks, proxied);
    }


    private void sendResult(Supplier<Response> processResponse,
                            Integer neededAcks,
                            AtomicInteger receivedAcks,
                            HttpSession session,
                            boolean proxied) {
        if (receivedAcks.getAcquire() >= neededAcks || proxied) {
            try {
                session.sendResponse(processResponse.get());
            } catch (IOException e) {
                session.close();
            }
        }
    }

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
    private void coordinateDelete(final String[] replicaNodes,
                                  @NotNull final HttpSession session,
                                  @NotNull final Request rqst,
                                  final int acks) {
        final String id = rqst.getParameter("id=");
        final boolean proxied = rqst.getHeader(PROXY_HEADER) != null;
        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final AtomicInteger asks = new AtomicInteger(0);
        final Function<HttpRequest.Builder, HttpRequest.Builder> methodDefiner = HttpRequest.Builder::DELETE;
        final Supplier<Response> successResponse = () -> new Response(Response.ACCEPTED, Response.EMPTY);
        final ArrayList<String> uris = new ArrayList<>(Arrays.asList(replicaNodes));
        if (uris.remove(nodes.getId())) {
            try {
                dao.removeRecordWithTimestamp(key);
                asks.incrementAndGet();
            } catch (IOException e) {
                try {
                    session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                } catch (IOException exp) {
                    session.close();
                }
            }
        }
        sendResult(successResponse, acks, asks, session, proxied);
        final List<HttpRequest> requests = Utils.createRequests(uris, rqst, methodDefiner);
        if (!uris.isEmpty()) {
            processPutAndDeleteRequest(requests, asks, successResponse, session, acks);
        }
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
        final Function<HttpRequest.Builder, HttpRequest.Builder> methodDefiner =
                x -> x.PUT(HttpRequest.BodyPublishers.ofByteArray(rqst.getBody()));
        final ArrayList<String> uris = new ArrayList<>(Arrays.asList(replicaNodes));
        final Supplier<Response> successResponse = () -> new Response(Response.CREATED, Response.EMPTY);
        if (uris.remove(nodes.getId())) {
            try {
                dao.upsertRecordWithTimestamp(key, ByteBuffer.wrap(rqst.getBody()));
                asks.incrementAndGet();
            } catch (IOException e) {
                try {
                    session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                } catch (IOException exp) {
                    session.close();
                }
            }
        }
        this.sendResult(successResponse, acks, asks, session, proxied);
        final List<HttpRequest> requests = Utils.createRequests(uris, rqst, methodDefiner);
        if (!uris.isEmpty()) {
            processPutAndDeleteRequest(requests, asks, successResponse, session, acks);
        }
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
        final ArrayList<String> uris = new ArrayList<>(Arrays.asList(replicaNodes));
        final List<TimestampRecord> responses = Collections.synchronizedList(new ArrayList<>());
        if (uris.remove(nodes.getId())) {
            try {
                try {
                    getTimestampRecordFromLocalDao(key, responses);
                } catch (NoSuchElementException exp) {
                    responses.add(TimestampRecord.getEmpty());
                }
                asks.incrementAndGet();
            } catch (IOException e) {
                try {
                    session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                } catch (IOException exp) {
                    session.close();
                }
            }
        }
        this.sendResult(() -> processResponses(responses, proxied), acks, asks, session, proxied);
        if (!uris.isEmpty()) {
            final List<HttpRequest> requests = Utils.createRequests(uris, rqst, methodDefiner);
            final List<CompletableFuture<Void>> futureList = requests.stream()
                    .map(request -> client.sendAsync(request, ofByteArray())
                            .thenAccept(response -> {
                                if (response.statusCode() == 404 && response.body().length == 0) {
                                    responses.add(TimestampRecord.getEmpty());
                                }
                                if (response.statusCode() == 500) return;
                                responses.add(TimestampRecord.fromBytes(response.body()));
                                asks.incrementAndGet();
                                this.sendResult(() -> processResponses(responses, proxied), acks, asks, session, proxied);
                            }))
                    .collect(Collectors.toList());
            checkResponses(futureList, session, acks, asks, proxied);
        }
    }

    private void getTimestampRecordFromLocalDao(final ByteBuffer key,
                                                final List<TimestampRecord> responses) throws IOException{
        final var record = TimestampRecord.fromBytes(copyAndExtractWithTimestampFromByteBuffer(key));
        responses.add(record);
    }
    private Response processResponses(final List<TimestampRecord> responses,
                                      final boolean proxied) {
        final TimestampRecord mergedResp = TimestampRecord.merge(responses);
        if (mergedResp.isValue()) {
            if (proxied) {
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
}
