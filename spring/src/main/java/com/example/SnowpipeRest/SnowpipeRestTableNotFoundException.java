package com.example.SnowpipeRest;

import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public class SnowpipeRestTableNotFoundException extends ResponseStatusException{
    public SnowpipeRestTableNotFoundException(String message) {
        super(HttpStatus.NOT_FOUND, message);
    }
}
