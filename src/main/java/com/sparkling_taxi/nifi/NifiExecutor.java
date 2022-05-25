package com.sparkling_taxi.nifi;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.FileBody;
import org.json.JSONArray;
import org.json.JSONException;
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

    /**
     * [TESTED] Uploads a template in the form of a ProcessGroup
     *
     * @return If successful returns the template id, otherwise Optional.empty()
     */
    public Optional<String> uploadTemplate(String file) {
        String url = "http://localhost:8181/nifi-api/process-groups/root/templates/upload";
        Optional<String> s = postNifiFile(file, url, "multipart/form-data");
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
        String templateToRemove = "http://localhost:8181/nifi-api/templates/" + templateId;
        return deleteNifi(templateToRemove);
    }

    /**
     * [TESTED] The list of processor groups currently instantiated in NiFi
     *
     * @return the list of processor groups in NiFi.
     */
    public List<String> getProcessorGroups() {
        String url = "http://localhost:8181/nifi-api/process-groups/" + getRootProcessGroup() + "/process-groups";
        Optional<JSONObject> response = getNifi(url);
        Optional<JSONArray> groups = response.map(r -> r.getJSONArray("processGroups"));
        return collectIDs(groups.orElse(new JSONArray()));
        // if groups are not null then collectIDs will return the list of ids
        // if groups are null, calls groups with an empty list (and it will return an empty list
    }

    /**
     * [TESTED only with a print] Get all processors in a processor group
     *
     * @param processGroup a processor group
     * @return the list of processor ids
     */
    public List<String> getProcessors(String processGroup) {
        String url = "http://localhost:8181/nifi-api/process-groups/" + processGroup + "/processors";
        Optional<JSONObject> response = getNifi(url);
        Optional<JSONArray> processes = response.map(r -> r.getJSONArray("processors"));
        return collectIDs(processes.orElse(new JSONArray()));
        // if processes are null, then calls collectIDs with an empty JSONArray and will return an empty list
        // otherwise collects the processors ids
    }

    /**
     * [TESTED] Instantiate a new template inside NiFi as a processor group
     *
     * @param templateId the template id
     * @return the Optional with the id of the instantiated ProcessorGroup.
     */
    public Optional<String> instantiateTemplate(String templateId) {
        String rootProcessGroup = getRootProcessGroup();
        String url = "http://localhost:8181/nifi-api/process-groups/" + rootProcessGroup + "/template-instance";
        Optional<JSONObject> response = postNifiImportTemplate(templateId, url, "application/json");

        // The map-filter chain on an Optional prevents NullPointerExceptions
        // if all goes well it returns an Optional with the id of the instantiated processorGroup
        // otherwise returns Optional.empty()
        try {
            return response.map(r -> r.getJSONObject("flow"))
                    .map(f -> f.getJSONArray("processGroups"))
                    .filter(p -> !p.isEmpty())
                    .map(p -> p.getJSONObject(0))
                    .map(p -> p.getString("id"));
        } catch (JSONException j) {
            System.err.println(j.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Returns a JSON object with the information of a processor group in the flow
     * @param processorGroupId id of the processor group in the flow
     * @return the json object in an Optional
     */
    public Optional<JSONObject> getProcessorGroupInfo(String processorGroupId) {
        String processorGroup = "http://localhost:8181/nifi-api/flow/process-groups/" + processorGroupId;
        return getNifi(processorGroup);
    }

    /**
     * Removes a processGroup from the flow given its id
     * @param processGroupId the id of the processGroup to remove from the flow
     * @return the response
     */
    public Optional<JSONObject> removeProcessGroup(String processGroupId) {
        // To remove a process group we need two parameters: revisionNumber and groupId.
        // The revision number is presumably always 0, but I can be wrong
        String processGroupToRemove = "http://localhost:8181/nifi-api/process-groups/" + processGroupId + "?version=0";
        return deleteNifi(processGroupToRemove);
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
     * Gets the root Process Group ID of Nifi
     *
     * @return the root ProcessGroup ID
     */
    public String getRootProcessGroup() {
        Optional<JSONObject> response = getNifi("http://localhost:8181/nifi-api/process-groups/root/");
        if (response.isPresent()) {
            return response.get().getString("id");
        }
        return "";
    }

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
    private static Optional<JSONObject> deleteNifi(String stringUrl) {
        try {
            System.out.println("DELETE " + stringUrl);
            URL url = new URL(stringUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod(HttpDelete.METHOD_NAME);
            conn.setRequestProperty("Content-Type", "application/json");
            System.out.println("DELETE " + conn.getResponseCode() + ": " + conn.getResponseMessage());
            InputStream is = conn.getInputStream();
            String response = IOUtils.toString(is, StandardCharsets.UTF_8);
            return Optional.of(new JSONObject(response));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

//    private static JSONObject putNifi(String stringUrl, String acceptType) {
//        try {
//            URL url = new URL(stringUrl);
//            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
//            connection.setDoOutput(true);
//            connection.setRequestMethod("PUT");
//            connection.setRequestProperty("Accept", "application/xml");
//            OutputStream out = connection.getOutputStream();
//            String response = out.toString();
//            System.out.println("response put:" + response);
//            int status = connection.getResponseCode();
//            System.out.println(status + ": " + connection.getResponseMessage());
//            return new JSONObject(response);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return null;
//    }

    /**
     * Posts (creates) a file on the NiFi server.
     *
     * @param stringUrl  url to NiFi
     * @param acceptType ex. application/json
     * @return the Optional with response String if all goes well
     */
    private static Optional<String> postNifiFile(String template_file, String stringUrl, String acceptType) {
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


    /**
     * Implements the instantiation of a template
     *
     * @param stringUrl  the complete REST URL
     * @param acceptType example "application/json"
     * @return the JSON response ot the method
     */
    private static Optional<JSONObject> postNifiImportTemplate(String templateId, String stringUrl, String acceptType) {
        System.out.println("POST " + stringUrl + " template=" + templateId);
        try {
            URL url = new URL(stringUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", acceptType);
            String jsonInputString = "{\n" +
                                     "    \"originX\": 2.0,\n" +
                                     "    \"originY\": 3.0,\n" +
                                     "    \"templateId\": \"" + templateId + "\"\n" +
                                     "}";
            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = jsonInputString.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            StringBuilder response = new StringBuilder();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                String responseLine;
                while ((responseLine = br.readLine()) != null) {
                    response.append(responseLine.trim());
                }
                System.out.println(response);
                System.out.println("POST " + conn.getResponseCode() + ": " + conn.getResponseMessage());
            }
            return Optional.of(new JSONObject(response.toString()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }
}
