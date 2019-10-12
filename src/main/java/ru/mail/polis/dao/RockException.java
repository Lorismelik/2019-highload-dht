package ru.mail.polis.dao;

import java.io.IOException;

public class RockException extends IOException {
    public RockException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
