package ru.mail.polis.service.lorismelik;

import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.lorismelik.NoSuchElementExceptionLite;
import ru.mail.polis.service.Service;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Path;
import one.nio.http.Response;
import one.nio.http.Request;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import java.io.IOException;

import java.util.logging.Logger;
import java.util.Iterator;
import java.util.concurrent.Executor;

import org.jetbrains.annotations.NotNull;

import static java.util.logging.Level.INFO;

public class AsyncServiceImpl extends HttpServer implements Service {
    @NotNull
    private final DAO dao;
    @NotNull
    private final Executor executor;

    private static final Logger logger = Logger.getLogger(AsyncServiceImpl.class.getName());

    /**
     * Simple Async HTTP server.
     *
     * @param port     - to accept HTTP connections
     * @param dao      - storage interface
     * @param executor - an object that executes submitted tasks
     */
    public AsyncServiceImpl(final int port, @NotNull final DAO dao, @NotNull final Executor executor)
            throws IOException {
        super(from(port));
        this.dao = dao;
        this.executor = executor;
    }

    private static HttpServerConfig from(final int port) {
        final AcceptorConfig ac = new AcceptorConfig();
        ac.port = port;
        ac.reusePort = true;
        ac.deferAccept = true;

        final HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{ac};
        return config;
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

        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
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
