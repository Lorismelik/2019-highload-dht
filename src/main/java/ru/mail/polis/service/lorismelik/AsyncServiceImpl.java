package ru.mail.polis.service.lorismelik;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import one.nio.pool.PoolException;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.lorismelik.NoSuchElementExceptionLite;
import ru.mail.polis.dao.lorismelik.RocksDAO;
import ru.mail.polis.service.Service;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Path;
import one.nio.http.Response;
import one.nio.http.Request;
import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.net.Socket;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import java.io.IOException;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.Iterator;
import java.util.concurrent.Executor;

import org.jetbrains.annotations.NotNull;

import static java.util.logging.Level.INFO;

public class AsyncServiceImpl extends HttpServer implements Service {
    @NotNull
    private final RocksDAO dao;
    @NotNull
    private final Executor executor;
    private final NodeDescriptor nodes;

    private final int clusterSize;
    private final Map<String, HttpClient> clusterClients;

    private static final Logger logger = Logger.getLogger(AsyncServiceImpl.class.getName());


    private static final String PROXY_HEADER = "X-OK-Proxy: True";
    private final RF defaultRF;

    /**
     * Create the HTTP Cluster server.
     *
     * @param config         HTTP server configurations
     * @param dao            to initialize the DAO instance within the server
     * @param nodes          to represent cluster nodes
     * @param clusterClients initialized cluster clients
     */
    public AsyncServiceImpl(final HttpServerConfig config, @NotNull final DAO dao,
                            @NotNull final NodeDescriptor nodes,
                            @NotNull final Map<String, HttpClient> clusterClients) throws IOException {
        super(config);
        this.dao = (RocksDAO) dao;
        this.executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
                new ThreadFactoryBuilder().setNameFormat("worker").build());
        this.nodes = nodes;
        this.clusterClients = clusterClients;
        this.defaultRF = new RF(nodes.getNodes().size() / 2 + 1, nodes.getNodes().size());
        this.clusterSize = nodes.getNodes().size();
    }

    @Override
    public HttpSession createSession(final Socket socket) {
        return new StorageSession(socket, this);
    }

    /**
     * Life check.
     *
     * @return response
     */
    @Path("/v0/status")
    public Response status() {
        return Response.ok("OK");
    }

    /**
     * Access to DAO.
     *
     * @param request requests: GET, PUT, DELETE
     * @param session HttpSession
     */
    private void entity(@NotNull final Request request, final HttpSession session) throws IOException {
        final String id = request.getParameter("id=");
        if (id == null || id.isEmpty()) {
            try {
                session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
            } catch (IOException e) {
                logger.log(INFO, "something has gone terribly wrong", e);
            }
            return;
        }

        boolean proxied = false;
        if (request.getHeader(PROXY_HEADER) != null) {
            proxied = true;
        }
        final String replicas = request.getParameter("replicas");
        final RF rf = RF.calculateRF(replicas, session, defaultRF, clusterSize);
        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final boolean proxiedF = proxied;

        if (proxied || nodes.getNodes().size() > 1) {
            final Coordinators clusterCoordinator = new Coordinators(nodes, clusterClients, dao, proxiedF);
            final String[] replicaClusters = proxied ? new String[]{nodes.getId()} : nodes.replicas(rf.getFrom(), key);
            clusterCoordinator.coordinateRequest(replicaClusters, request, rf.getAck(), session);
        } else {
            try {
                switch (request.getMethod()) {
                    case Request.METHOD_GET:
                        executeAsync(session, () -> doGet(key));
                        break;
                    case Request.METHOD_PUT:
                        executeAsync(session, () -> {
                            dao.upsert(key, ByteBuffer.wrap(request.getBody()));
                            return new Response(Response.CREATED, Response.EMPTY);
                        });
                        break;
                    case Request.METHOD_DELETE:
                        executeAsync(session, () -> {
                            dao.remove(key);
                            return new Response(Response.ACCEPTED, Response.EMPTY);
                        });
                        break;
                    default:
                        session.sendError(Response.METHOD_NOT_ALLOWED, "Wrong method");
                        break;
                }
            } catch (IOException e) {
                session.sendError(Response.INTERNAL_ERROR, e.getMessage());
            }
        }
    }

    @Override
    public void handleDefault(@NotNull final Request request, @NotNull final HttpSession session) throws IOException {
        switch (request.getPath()) {
            case "/v0/entity":
                entity(request, session);
                break;
            case "/v0/entities":
                entities(request, session);
                break;
            default:
                session.sendError(Response.BAD_REQUEST, "Wrong path");
                break;
        }
    }

    private void executeAsync(@NotNull final HttpSession session, @NotNull final Action action) {
        executor.execute(() -> {
            try {
                session.sendResponse(action.act());
            } catch (IOException e) {
                try {
                    session.sendError(Response.INTERNAL_ERROR, e.getMessage());
                } catch (IOException ex) {
                    logger.log(INFO, "something has gone terribly wrong", e);
                }
            }
        });
    }

    @FunctionalInterface
    interface Action {
        Response act() throws IOException;
    }

    private void entities(@NotNull final Request request, @NotNull final HttpSession session) throws IOException {
        final String start = request.getParameter("start=");
        if (start == null || start.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "No start");
            return;
        }

        if (request.getMethod() != Request.METHOD_GET) {
            session.sendError(Response.METHOD_NOT_ALLOWED, "Wrong method");
            return;
        }

        String end = request.getParameter("end=");
        if (end != null && end.isEmpty()) {
            end = null;
        }

        try {
            final Iterator<Record> records =
                    dao.range(ByteBuffer.wrap(start.getBytes(StandardCharsets.UTF_8)),
                            end == null ? null : ByteBuffer.wrap(end.getBytes(StandardCharsets.UTF_8)));
            ((StorageSession) session).stream(records);
        } catch (IOException e) {
            session.sendError(Response.INTERNAL_ERROR, e.getMessage());
        }
    }

    private Response doGet(final ByteBuffer key) {
        try {
            final var value = dao.get(key).duplicate();
            value.arrayOffset();
            return new Response(Response.OK, value.array());
        } catch (NoSuchElementExceptionLite | IOException ex) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }
}
