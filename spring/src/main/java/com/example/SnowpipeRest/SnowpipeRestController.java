package com.example.SnowpipeRest;

import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/snowpipe")
public class SnowpipeRestController {
    @Autowired
    private SnowpipeRestRepository repos;

    @PutMapping("/insert/{database}/{schema}/{table}")
    @ResponseBody
    public String insert(@PathVariable String database, @PathVariable String schema, @PathVariable String table, @RequestBody String body) {
        SnowpipeInsertResponse sp_resp = repos.saveToSnowflake(database, schema, table, body);
        return sp_resp.toString();
    }

    @ExceptionHandler(SnowpipeRestTableNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ResponseEntity<String> handleTableNotFound(SnowpipeRestTableNotFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage());
    }

    @ExceptionHandler(SnowpipeRestJsonParseException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ResponseEntity<String> handleBadJson(SnowpipeRestJsonParseException e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
    }
}
