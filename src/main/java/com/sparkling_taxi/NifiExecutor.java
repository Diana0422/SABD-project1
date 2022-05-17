package com.sparkling_taxi;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.shaded.com.squareup.okhttp.MultipartBuilder;
import org.apache.hadoop.shaded.org.apache.http.client.HttpClient;
import org.apache.hadoop.shaded.org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.hadoop.shaded.org.apache.http.client.methods.HttpPost;
import org.apache.hadoop.shaded.org.apache.http.impl.client.CloseableHttpClient;
import org.apache.hadoop.shaded.org.apache.http.impl.client.DefaultHttpClient;
import org.apache.hadoop.shaded.org.apache.http.impl.client.HttpClients;
import org.apache.http.HttpResponse;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.FileBody;
import org.json.JSONArray;
import org.json.JSONObject;
import scala.util.parsing.json.JSON;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class NifiExecutor {

    private String processGroup;
    private List<String> processGroups;
    private List<String> processors;

    public NifiExecutor() {
        this.processGroup = getProcessGroup();
        this.processGroups = getProcessGroups();
        this.processors = getProcessors(processGroup);
    }

    private List<String> getProcessGroups() {
        String url = GET_PROCESS_GROUPS + processGroup + "/process-groups";
        JSONObject response = getNifi(url);
        JSONArray groups = response.getJSONArray("processGroups");
        return collectIDs(groups);
    }

    private List<String> getProcessors(String processGroup) {
        String url = GET_PROCESSORS + processGroup + "/processors";
        JSONObject response = getNifi(url);
        JSONArray process = response.getJSONArray("processors");
        return collectIDs(process);
    }

    private JSONObject instantiateTemplate(String templateId) {
        String url = GET_PROCESS_GROUPS + templateId + "/templates/import";
        JSONObject response = postNifi(url, "application/xml");
        return response;
    }

    private JSONObject uploadTemplate() {
        String url = GET_PROCESS_GROUPS + processGroup + "/templates/upload";
        JSONObject response = postNifi(url, "multipart/form-data");
        return response;
    }

    private List<String> collectIDs(JSONArray array) {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < array.length(); i++) {
            JSONObject object = array.getJSONObject(i);
            ids.add(object.getString("id"));
        }
        return ids;
    }

    private String getProcessGroup() {
        JSONObject response = getNifi(GET_PROCESS_GROUP);
        return response.getString("id");
    }

    public static String GET_PROCESS_GROUP = "http://localhost:8181/nifi-api/process-groups/root/"; // retrieve process group id
    public static String GET_PROCESSORS = "http://localhost:8181/nifi-api/process-groups/";
    public static String GET_PROCESS_GROUPS = "http://localhost:8181/nifi-api/process-groups/";

    public static void main(String[] args) {
        /* supponendo di aver già caricato il template in nifi */
        // recupera il process group istanziato
        NifiExecutor executor = new NifiExecutor();
        String group = executor.getProcessGroup();
        executor.uploadTemplate();
        List<String> groups = executor.getProcessGroups();
        executor.instantiateTemplate(groups.get(0));
        System.out.println(groups);
        for (String g : groups) {
            System.out.println(executor.getProcessors(g));
        }
    }

    private static JSONObject getNifi(String stringUrl) {
        try {
            System.out.println(stringUrl);
            URL url = new URL(stringUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Accept", "application/json");
            InputStream is = connection.getInputStream();
            String response = IOUtils.toString(is, "UTF-8");
            return new JSONObject(response);
        } catch (IOException e) {
            //TODO handle exception
            e.printStackTrace();
        }
        return null;
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
            System.out.println("response put:"+response);
            int status = connection.getResponseCode();
            System.out.println(status+": "+connection.getResponseMessage());
//            return new JSONObject(response);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static JSONObject postNifi(String stringUrl, String acceptType) {
        try {
            File file = new File(".nifi/templates/multi_file_preprocessing_group.xml");
            URL url = new URL(stringUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setRequestMethod("POST");

            if (acceptType.equals("multipart/form-data")) {
                FileBody fileBody = new FileBody(file);
                MultipartEntity multipartEntity = new MultipartEntity(HttpMultipartMode.STRICT);
                multipartEntity.addPart("template", fileBody);

                connection.setRequestProperty("Content-Type", multipartEntity.getContentType().getValue());
                OutputStream out = connection.getOutputStream();
                try {
                    multipartEntity.writeTo(out);
                } finally {
                    out.close();
                }
            } else {
                connection.setRequestProperty("Content-Type", acceptType);
            }
            int status = connection.getResponseCode();
            System.out.println(status+": "+connection.getResponseMessage());
            if (status == HttpURLConnection.HTTP_OK) {
                BufferedReader inputReader = new BufferedReader(
                        new InputStreamReader(connection.getInputStream()));
                String inputLine;
                StringBuffer response = new StringBuffer();

                while ((inputLine = inputReader.readLine()) != null) {
                    response.append(inputLine);
                }
                inputReader.close();
                System.out.println(response);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
