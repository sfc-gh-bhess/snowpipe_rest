package com.example.SnowpipeRest;

import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public class SnowpipeRestJsonParseException extends ResponseStatusException {
    public SnowpipeRestJsonParseException(String message) {
        super(HttpStatus.BAD_REQUEST, message);
    }
}
