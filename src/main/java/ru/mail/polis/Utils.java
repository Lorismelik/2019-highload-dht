package ru.mail.polis;

import one.nio.http.Request;

import java.net.URI;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.List;
import java.util.function.Function;

import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.stream.Collectors.toList;

public abstract class Utils {

    @FunctionalInterface
    public interface MyConsumer<T, U, R, Y, C> {
        void accept(T t, U u, R r, Y y, C c);
    }

    /**
     * Create list of requests which will be send to other nodes
     *
     * @param uris    addresses of other nodes
     * @param rqst    http request which was accepted from client
     * @param methodDefiner define http method of requests
     */
    public static List<HttpRequest> createRequests(final List<String> uris,
                                                   final Request rqst,
                                                   final Function<HttpRequest.Builder, HttpRequest.Builder> methodDefiner)  {
        return uris.stream()
                .map(x -> x + rqst.getURI())
                .map(Utils::createURI)
                .map(HttpRequest::newBuilder)
                .map(x -> x.setHeader("X-OK-Proxy", "true"))
                .map(methodDefiner)
                .map(x -> x.timeout(Duration.of(5, SECONDS)))
                .map(HttpRequest.Builder::build)
                .collect(toList());
    }

    private static URI createURI(final String s) {
        try {
            return URI.create(s);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
    }
}
