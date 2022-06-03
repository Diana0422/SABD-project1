package com.sparkling_taxi.nifi;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.json.JSONObject;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class NifiREST {
    private static final boolean VERBOSE = false;

    static boolean putNifi(String stringUrl, String body) {
        try {
            URL url = new URL(stringUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            System.out.println("stringUrl = " + stringUrl + ", body = " + body);
            conn.setDoOutput(true);
            conn.setRequestMethod("PUT");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept", "application/json");
            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = body.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }
            if (VERBOSE) {
                StringBuilder response = new StringBuilder();
                try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                    String responseLine;
                    while ((responseLine = br.readLine()) != null) {
                        response.append(responseLine.trim());
                    }
                    System.out.println(response);
                } catch (IOException io) {
                    io.printStackTrace();
                    try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getErrorStream(), StandardCharsets.UTF_8))) {
                        String responseLine;
                        while ((responseLine = br.readLine()) != null) {
                            response.append(responseLine.trim());
                        }
                        System.out.println(response);
                    }
                }
            }
            int status = conn.getResponseCode();
            return conn.getResponseCode() == 200;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }


    static Optional<JSONObject> getNifi(String stringUrl) {
        try {
            System.out.println("GET " + stringUrl);
            URL url = new URL(stringUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");
            InputStream is = conn.getInputStream();
            String response = IOUtils.toString(is, StandardCharsets.UTF_8);
            return Optional.of(new JSONObject(response));
        } catch (IOException e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }

    /**
     * Deletes a resource
     *
     * @param stringUrl the REST API url
     * @return the response or Optional.empty()
     */
    static boolean deleteNifi(String stringUrl, boolean withJson) {
        try {
            System.out.println("DELETE " + stringUrl);
            URL url = new URL(stringUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod(HttpDelete.METHOD_NAME);
            if (withJson)
                conn.setRequestProperty("Content-Type", "application/json");
            int code = conn.getResponseCode();
            if (VERBOSE) {
                InputStream is;
                if (code == 200) {
                    is = conn.getInputStream();
                } else {
                    is = conn.getErrorStream();
                }
                String response = IOUtils.toString(is, StandardCharsets.UTF_8);
                System.out.println(response);
            }
            return code == 200;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * Posts (creates) a file on the NiFi server.
     *
     * @param stringUrl url to NiFi
     * @return the Optional with response String if all goes well
     */
    static Optional<String> postNifiFile(String template_file, String stringUrl) {
        try {
            System.out.println("POST " + stringUrl + " file=" + template_file);
            File file = new File(template_file);
            URL url = new URL(stringUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            FileBody fileBody = new FileBody(file);
            HttpEntity httpEntity = MultipartEntityBuilder.create()
                    .setMode(HttpMultipartMode.BROWSER_COMPATIBLE)
                    .addPart("template", fileBody)
                    .build();

            conn.setRequestProperty("Content-Type", httpEntity.getContentType().getValue());
            try (OutputStream out = conn.getOutputStream()) {
                httpEntity.writeTo(out);
            }
            BufferedReader inputReader = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()));
            String inputLine;
            StringBuilder response = new StringBuilder();

            while ((inputLine = inputReader.readLine()) != null) {
                response.append(inputLine);
            }
            inputReader.close();
            return Optional.of(response.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }


    /**
     * Implements the instantiation of a template
     *
     * @param stringUrl the complete REST URL
     * @return the JSON response ot the method
     */
    static Optional<JSONObject> postNifiImportTemplate(String stringUrl, String json) {
        System.out.println("POST " + stringUrl);
        try {
            URL url = new URL(stringUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            if (json != null) {
                try (OutputStream os = conn.getOutputStream()) {
                    byte[] input = json.getBytes(StandardCharsets.UTF_8);
                    os.write(input, 0, input.length);
                }
            }

            StringBuilder response = new StringBuilder();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                String responseLine;
                while ((responseLine = br.readLine()) != null) {
                    response.append(responseLine.trim());
                }
                if (VERBOSE) {
                    System.out.println(response);
                }
            }
            return Optional.of(new JSONObject(response.toString()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }
}
