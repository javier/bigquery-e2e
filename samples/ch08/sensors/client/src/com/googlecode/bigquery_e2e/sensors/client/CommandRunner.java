package com.googlecode.bigquery_e2e.sensors.client;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.json.JSONException;
import org.json.JSONObject;

import android.util.Log;

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

  JSONObject run(String command, JSONObject arg) throws ErrorResult {
    JSONObject result = new JSONObject();
    int responseCode = 500;
    HttpURLConnection conn = null;
    try {
      result.put("error", "NotImplemented");
      result.put("message", "Method has not been implemented.");
      String path = "/command/" + command;
      URL url = host.getPort() == -1 ? new URL(host.getScheme(),
              host.getHost(), path) : new URL(host.getScheme(), host.getHost(),
              host.getPort(), path);
      conn = (HttpURLConnection) url.openConnection();
      byte body[] = arg.toString().getBytes();
      conn.setConnectTimeout(60 * 1000);
      conn.setReadTimeout(60 * 1000);
      conn.setRequestMethod("POST");
      conn.setRequestProperty("User-Agent",
        CommandRunner.class.getCanonicalName());
      conn.setRequestProperty("Content-Type", "application/json");
      conn.setDoInput(true);
      conn.setDoOutput(true);
      conn.setFixedLengthStreamingMode(body.length);
      conn.connect();
      OutputStream os = conn.getOutputStream();
      os.write(body);
      os.close();
      responseCode = conn.getResponseCode();
      InputStreamReader is = new InputStreamReader(
          conn.getInputStream(), "UTF-8");
      int contentLength = conn.getContentLength();
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
      is.close();
      result = new JSONObject(builder.toString());
      if (responseCode != 200 || result.has("error")) {
        throw new ErrorResult(result);
      }
    } catch (MalformedURLException e) {
      throw new ErrorResult(e);
    } catch (IOException e) {
      throw new ErrorResult(e);
    } catch (JSONException e) {
      throw new ErrorResult(e);
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }
    return result;
  }
}
