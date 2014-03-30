package com.googlecode.bigquery_e2e.sensors.client;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.json.JSONException;
import org.json.JSONObject;

class CommandRunner {
  private final URI host;

  static class ErrorResult extends Exception {
    private static final long serialVersionUID = 1L;
    private final String error;

    ErrorResult(JSONObject error) {
      super(error.optString("message"));
      this.error = error.optString("error", "Unknown");
    }

    ErrorResult(Throwable ex) {
      super(ex);
      this.error = ex.getClass().getSimpleName();
    }

    String getError() {
      return error;
    }
  }

  CommandRunner(String host) {
    try {
      this.host = new URI("http://" + host);
    } catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
  }

  /** 
   * Sends a command and decodes the response.
   * 
   * @param command identifies the remote operation.
   * @param arg JSON argument for the remote operation.
   * @return result of the remote operation.
   * @throws ErrorResult if there was an error running the operation.
   */
  JSONObject run(String command, JSONObject arg) throws ErrorResult {
    JSONObject result = notImplemented();
    HttpURLConnection conn = createConnection(command);
    try {
      byte body[] = arg.toString().getBytes();
      int responseCode = sendRequest(conn, body);
      String response = readResponse(conn);
      try {
        result = responseCode == 200 ?
                new JSONObject(response) :
                connectionError(responseCode, response);
      } catch (JSONException ex) {
        throw new ErrorResult(ex);
      }
    } finally {
      conn.disconnect();
    }
    if (result.has("error")) {
      throw new ErrorResult(result);
    }
    return result;
  }
  
  private JSONObject notImplemented() {
    JSONObject result = new JSONObject();
    try {
      result.put("error", "NotImplemented");
      result.put("message", "Method has not been implemented.");
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
    return result;
  }
  
  private JSONObject connectionError(int code, String body) {
    JSONObject result = new JSONObject();
    try {
      result.put("error", "ConnectionError");
      result.put("message", String.format("Code = %d: %s", code, body));
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
    return result;    
  }
  
  private HttpURLConnection createConnection(String command)
          throws ErrorResult {
    String path = "/command/" + command;
    try {
      URL url = host.getPort() == -1 ?
              new URL(host.getScheme(), host.getHost(), path) :
              new URL(host.getScheme(), host.getHost(), host.getPort(), path);
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setConnectTimeout(60 * 1000);
      conn.setReadTimeout(60 * 1000);
      conn.setRequestProperty(
        "User-Agent", CommandRunner.class.getCanonicalName());
      conn.setDoInput(true);
      conn.setDoOutput(true);
      return conn;
    } catch (IOException ex) {
      throw new ErrorResult(ex);
    }
  }

  private int sendRequest(HttpURLConnection conn, byte[] body)
          throws ErrorResult {
    conn.setRequestProperty("Content-Type", "application/json");
    conn.setFixedLengthStreamingMode(body.length);
    try {
      conn.setRequestMethod("POST");
      conn.connect();
      OutputStream os = conn.getOutputStream();
      try {
        os.write(body);
      } finally {
        os.close();
      }
      return conn.getResponseCode();
    } catch (IOException ex) {
      throw new ErrorResult(ex);
    }
  }

  private String readResponse(HttpURLConnection conn)
          throws ErrorResult {
    int contentLength = conn.getContentLength();
    try {      
      InputStreamReader is = new InputStreamReader(conn.getInputStream(), "UTF-8");
      try {
        StringBuilder builder = new StringBuilder();
        char buffer[];
        if (contentLength > 0) {
          buffer = new char[contentLength];
        } else {
          buffer = new char[512];
        }
        int charsRead;
        while ((charsRead = is.read(buffer)) > 0) {
          builder.append(buffer, 0, charsRead);
        }
        return builder.toString();
      } finally {
        is.close();
      }
    } catch (IOException ex) {
      throw new ErrorResult(ex);    
    }
  }
}
