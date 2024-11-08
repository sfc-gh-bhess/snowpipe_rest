package com.example.SnowpipeRest;

import java.util.ArrayList;
import java.util.List;

public class SnowpipeInsertResponse {
    int num_attempted;
    int num_succeeded;
    int num_errors;
    List<SnowpipeInsertError> errors;

    public SnowpipeInsertResponse(int num_attempted, int num_succeeded, int num_errors) {
        this(num_attempted, num_succeeded, num_errors, new ArrayList<SnowpipeInsertError>());
    }

    public SnowpipeInsertResponse(int num_attempted, int num_succeeded, int num_errors, List<SnowpipeInsertError> errors) {
        this.num_attempted = num_attempted;
        this.num_succeeded = num_succeeded;
        this.num_errors = num_errors;
        this.errors = errors;
    }

    public int getNum_attempted() {
        return num_attempted;
    }

    public int getNum_succeeded() {
        return num_succeeded;
    }

    public int getNum_errors() {
        return num_errors;
    }

    public void setNum_attempted(int n) {
        this.num_attempted = n;
    }

    public void setNum_succeeded(int n) {
        this.num_succeeded = n;
    }

    public void setNum_errors(int n) {
        this.num_errors = n;
    }

    public void add_metrics(int n_attempted, int n_succeeded, int n_errors) {
        this.num_attempted += n_attempted;
        this.num_succeeded += n_succeeded;
        this.num_errors += n_errors;
    }

    public List<SnowpipeInsertError> getErrors() {
        return errors;
    }

    public SnowpipeInsertResponse addError(int row_index, String input, String error) {
        return addError(new SnowpipeInsertError(row_index, input, error));
    }

    public SnowpipeInsertResponse addError(SnowpipeInsertError e) {
        errors.add(e);
        return this;
    }

    public String toString() {
        StringBuffer resp_body = new StringBuffer("{\n");
        resp_body.append(String.format(
                "  \"inserts_attempted\": %d,\n  \"inserts_succeeded\": %d,\n  \"insert_errors\": %d,\n",
                num_attempted, num_succeeded, num_errors));
        resp_body.append("  \"error_rows\":\n    [");
        String delim = " ";
        for (SnowpipeInsertError e: errors) {
            resp_body.append(String.format("\n    %s %s", delim, e.toString()));
            delim = ",";
        }
        resp_body.append("\n    ]");
        resp_body.append("\n}");
        return resp_body.toString();
    }
}
