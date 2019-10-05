package ru.mail.polis.dao;

import java.io.IOException;

public class RockException extends IOException {
    public RockException(String message, Throwable cause) {
        super(message, cause);
    }
}
