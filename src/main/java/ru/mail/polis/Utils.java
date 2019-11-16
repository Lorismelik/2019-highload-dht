package ru.mail.polis;

import one.nio.http.Request;

import java.net.URI;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.List;
import java.util.function.Function;

import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.stream.Collectors.toList;

public class Utils {

    @FunctionalInterface
    public interface FunctionWithException<T, R, E extends Exception> {
        R apply(T t) throws E;
    }

    /**
     * Wrapper to avoid try/catch exceptions in a stream
     *
     * @param fe function in a stream, which can throw exception
     */
    private static <T, R, E extends Exception>
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

    /**
     * Create list of requests which will be send to other nodes
     *
     * @param uris    addresses of other nodes
     * @param rqst    http request which was accepted from client
     * @param methodDefiner define http method of requests
     */
    public static List<HttpRequest> createRequests(List<String> uris,
                                             Request rqst,
                                             Function<HttpRequest.Builder, HttpRequest.Builder> methodDefiner)  {
        return uris.stream()
                .map(x -> x + rqst.getURI())
                .map(Utils.wrapper(URI::new))
                .map(HttpRequest::newBuilder)
                .map(x -> x.setHeader("X-OK-Proxy", "true"))
                .map(methodDefiner)
                .map(x -> x.timeout(Duration.of(5, SECONDS)))
                .map(HttpRequest.Builder::build)
                .collect(toList());
    }
}
