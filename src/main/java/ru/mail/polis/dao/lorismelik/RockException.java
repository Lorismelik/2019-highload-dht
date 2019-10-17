package ru.mail.polis.dao.lorismelik;

import java.io.IOException;

public class RockException extends IOException {
    private static final long serialVersionUID = 690367782597439433L;

    public RockException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
