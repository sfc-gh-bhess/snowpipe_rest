package com.example.SnowpipeRest;

public class SnowpipeInsertError {
    public int row_index;
    public String input;
    public String error;

    public SnowpipeInsertError(int row_index, String input, String error) {
        this.row_index = row_index;
        this.input = input;
        this.error = error;
    }

    public int getRow_index() {
        return row_index;
    }

    public String getInput() {
        return input;
    }

    public String getError() {
        return error;
    }

    public String toString() {
        return String.format("{\"row_index\": \"%s\", \"input\": \"%s\", \"error\": \"%s\"}", row_index, input, error);
    }
}
