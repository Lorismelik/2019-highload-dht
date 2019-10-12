package ru.mail.polis.service;

import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.Response;
import one.nio.http.Request;
import one.nio.http.HttpSession;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ServiceImpl extends HttpServer implements Service {
    private final DAO dao;

    ServiceImpl(final HttpServerConfig config, @NotNull final DAO dao) throws IOException {
        super(config);
        this.dao = dao;
    }

    /**
     * status response.
     *
     * @return Response
     */
    @Path("/v0/status")
    public Response status() {
        return Response.ok("OK");
    }

    /**
     * entity response.
     *
     * @return Response
     */
    @Path("/v0/entity")
    public Response entity(
            @Param("id") final String id,
            @NotNull final Request request
    ) {
        try {
            if (id == null || id.isEmpty()) {
                return new Response(Response.BAD_REQUEST, "Id must be not null".getBytes(StandardCharsets.UTF_8));
            }
            final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
            switch (request.getMethod()) {
                case Request.METHOD_GET: {
                        final var value = dao.get(key).duplicate();
                        return new Response(Response.OK, value.array());
                }
                case Request.METHOD_PUT: {
                    final var value = ByteBuffer.wrap(request.getBody());
                    dao.upsert(key, value);
                    return new Response(Response.CREATED, Response.EMPTY);
                }
                case Request.METHOD_DELETE: {
                    dao.remove(key);
                    return new Response(Response.ACCEPTED, Response.EMPTY);
                }
                default:
                    return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
            }
        } catch (IOException ex) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        final var response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }
}
