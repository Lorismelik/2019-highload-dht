package ru.mail.polis.dao.lorismelik;

import java.util.NoSuchElementException;

public class NoSuchElementExceptionLite extends NoSuchElementException {

    private static final long serialVersionUID = -6243961333992038198L;

    NoSuchElementExceptionLite(final String s) {
        super(s);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
