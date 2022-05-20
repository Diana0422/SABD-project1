package com.sparkling_taxi;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.FileBody;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NifiExecutor {

    private String processGroup;
    private List<String> processGroups;
    private List<String> processors;

    private static final String EXAMPLE_TEMPLATE = ".nifi/templates/multi_file_preprocessing_group.xml";

    public NifiExecutor() {
        //this.processGroup = getRootProcessGroup();
        //this.processGroups = getProcessorGroups();
        //this.processors = getProcessors(processGroup);
    }

    /**
     * [TESTED] Uploads a template in the form of a ProcessGroup
     *
     * @return If successful returns the template id, otherwise Optional.empty()
     */
    public Optional<String> uploadTemplate(String file) {
        String url = GET_ROOT_PROCESSOR_GROUP + "templates/upload";
        Optional<String> s = postNifi(EXAMPLE_TEMPLATE, url, "multipart/form-data");
        if (s.isPresent()) {
            // Matcher m = Pattern.compile("<groupId>(.*)</groupId>").matcher(s.get());
            Matcher m = Pattern.compile("<id>(.*)</id>").matcher(s.get());
            if (m.find()) {
                return Optional.of(m.group(1));
            }
        }
        return Optional.empty();
    }

    /**
     * [TESTED] Deletes a template given its id
     *
     * @return if succesful returns the response
     */
    public Optional<JSONObject> deleteTemplate(String templateId) {
        String templateToRemove = DELETE_TEMPLATE + templateId;
        return deleteNifi(templateToRemove);
    }

    /**
     * A bit useless
     * @return the list of processor groups in NiFi.
     */
    public List<String> getProcessorGroups() {
        String url = GET_ROOT_PROCESSOR_GROUP;
        Optional<JSONObject> response = getNifi(url);
        Optional<JSONArray> groups = response.map(r -> r.getJSONArray("processGroups"));
        return collectIDs(groups.orElse(new JSONArray()));
        // if groups are not null then collectIDs will return the list of ids
        // if groups are null, calls groups with an empty list (and it will return an empty list
    }

    /**
     * Get all processors in a processor group
     *
     * @param processGroup a processor group
     * @return the list of processor ids
     */
    public List<String> getProcessors(String processGroup) {
        String url = GET_PROCESSORS + processGroup + "/processors";
        Optional<JSONObject> response = getNifi(url);
        Optional<JSONArray> processes = response.map(r -> r.getJSONArray("processors"));
        return collectIDs(processes.orElse(new JSONArray()));
        // if processes are null, then calls collectIDs with an empty JSONArray and will return an empty list
        // otherwise collects the processors ids
    }

    /**
     * Instantiate a new template inside NiFi
     *
     * @param templateId the template id
     * @return the response of the operation
     */
    public Optional<String> instantiateTemplate(String templateId) {
        String url = GET_ROOT_PROCESSOR_GROUP + templateId + "/templates/import";
        return postNifi(EXAMPLE_TEMPLATE, url, "application/xml");
    }

    private List<String> collectIDs(JSONArray array) {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < array.length(); i++) {
            JSONObject object = array.getJSONObject(i);
            ids.add(object.getString("id"));
        }
        return ids;
    }

    /**
     * TODO: maybe prende il processGroup radice
     *
     * @return
     */
    public String getRootProcessGroup() {
        Optional<JSONObject> response = getNifi(GET_ROOT_PROCESSOR_GROUP);
        if (response.isPresent()) {
            return response.get().getString("id");
        }
        return "";
    }

    public static String GET_ROOT_PROCESSOR_GROUP = "http://localhost:8181/nifi-api/process-groups/root/"; // retrieve process group id
    public static String GET_PROCESSORS = "http://localhost:8181/nifi-api/process-groups/";
    // public static String GET_TEMPLATE = "http://localhost:8181/nifi-api/process-groups/root";
    // http://localhost:8181/nifi-api/process-groups/e030c09e-0180-1000-9d08-2a9c449ff2fa/

    public static String DELETE_TEMPLATE = "http://localhost:8181/nifi-api/templates/";

    private static Optional<JSONObject> getNifi(String stringUrl) {
        try {
            System.out.println("GET " + stringUrl);
            URL url = new URL(stringUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");
            InputStream is = conn.getInputStream();
            String response = IOUtils.toString(is, StandardCharsets.UTF_8);
            System.out.println("GET " + conn.getResponseCode() + ": " + conn.getResponseMessage());
            return Optional.of(new JSONObject(response));
        } catch (IOException e) {
            //TODO handle exception
            e.printStackTrace();
        }
        return Optional.empty();
    }


    /**
     * Deletes a resource
     *
     * @param stringUrl the REST API url
     * @return the response or Optional.empty()
     */
    private static Optional<JSONObject> deleteNifi(String stringUrl) {
        try {
            System.out.println("DELETE " + stringUrl);
            URL url = new URL(stringUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod(HttpDelete.METHOD_NAME);
            conn.setRequestProperty("Content-Type", "application/json");
            InputStream is = conn.getInputStream();
            String response = IOUtils.toString(is, StandardCharsets.UTF_8);
            System.out.println("DELETE " + conn.getResponseCode() + ": " + conn.getResponseMessage());
            return Optional.of(new JSONObject(response));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    private static JSONObject putNifi(String stringUrl, String acceptType) {
        try {
            URL url = new URL(stringUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Accept", "application/xml");
            OutputStream out = connection.getOutputStream();
            String response = out.toString();
            System.out.println("response put:" + response);
            int status = connection.getResponseCode();
            System.out.println(status + ": " + connection.getResponseMessage());
//            return new JSONObject(response);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * TODO: correggi il valore di ritorno...
     *
     * @param stringUrl
     * @param acceptType
     * @return FIXME: per ora sempre Optional.empty()
     */
    private static Optional<String> postNifi(String template_file, String stringUrl, String acceptType) {
        try {
            System.out.println("POST " + stringUrl + " file=" + template_file);
            File file = new File(template_file);
            URL url = new URL(stringUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");

            if (acceptType.equals("multipart/form-data")) {
                FileBody fileBody = new FileBody(file);
                MultipartEntity multipartEntity = new MultipartEntity(HttpMultipartMode.STRICT);
                multipartEntity.addPart("template", fileBody);

                conn.setRequestProperty("Content-Type", multipartEntity.getContentType().getValue());
                try (OutputStream out = conn.getOutputStream()) {
                    multipartEntity.writeTo(out);
                }
            } else {
                conn.setRequestProperty("Content-Type", acceptType);
            }
            System.out.println("POST " + conn.getResponseCode() + ": " + conn.getResponseMessage());

            BufferedReader inputReader = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()));
            String inputLine;
            StringBuilder response = new StringBuilder();

            while ((inputLine = inputReader.readLine()) != null) {
                response.append(inputLine);
            }
            inputReader.close();
            return Optional.of(response.toString());
            //  }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

}
