package com.sparkling_taxi.nifi;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.sparkling_taxi.nifi.NifiREST.*;

public class NifiExecutor {

    private String rootProcessGroupCache = "";
    private String nifiApiUrl;

    public NifiExecutor(String apiUrl){
        this.nifiApiUrl = apiUrl;
    }
    /**
     * [TESTED] Uploads a template in the form of a ProcessGroup
     *
     * @return If successful returns the template id, otherwise Optional.empty()
     */
    public Optional<String> uploadTemplate(String file) {
        String url = nifiApiUrl + "process-groups/root/templates/upload";
        Optional<String> s = NifiREST.postNifiFile(file, url);
        if (s.isPresent()) {
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
    public boolean deleteTemplate(String templateId) {
        String templateToRemove = nifiApiUrl + "templates/" + templateId;
        return deleteNifi(templateToRemove, true);
    }

    /**
     * Gets the root Process Group ID of Nifi
     *
     * @return the root ProcessGroup ID
     */
    public String getRootProcessGroup() {
        if (!rootProcessGroupCache.isEmpty()) {
            return rootProcessGroupCache;
        }
        // http://localhost:8181/nifi-api/process-groups/root/
        Optional<JSONObject> response = getNifi(nifiApiUrl + "process-groups/root/");
        response.ifPresent(jsonObject -> rootProcessGroupCache = jsonObject.getString("id"));
        return rootProcessGroupCache;
    }

    /**
     * [TESTED] Instantiate a new template inside NiFi as a processor group
     *
     * @param templateId the template id
     * @return the Optional with the id of the instantiated ProcessorGroup.
     */
    public Optional<String> instantiateTemplate(String templateId) {
        String rootProcessGroup = getRootProcessGroup();
        String url = nifiApiUrl + "process-groups/" + rootProcessGroup + "/template-instance";
        String jsonInputString = "{\n" +
                                 "    \"originX\": 2.0,\n" +
                                 "    \"originY\": 3.0,\n" +
                                 "    \"templateId\": \"" + templateId + "\"\n" +
                                 "}";
        Optional<JSONObject> response = NifiREST.postNifiImportTemplate(url, jsonInputString);

        // The map-filter chain on an Optional prevents NullPointerExceptions
        // if all goes well it returns an Optional with the id of the instantiated processorGroup
        // otherwise returns Optional.empty()
        try {
            return response.map(r -> r.getJSONObject("flow"))
                    .map(f -> f.getJSONArray("processGroups"))
//                    .filter(p -> !p.isEmpty())
                    .filter(p -> p.length() > 0)
                    .map(p -> p.getJSONObject(0))
                    .map(p -> p.getString("id"));
        } catch (JSONException j) {
            System.err.println(j.getMessage());
            JSONArray x = new JSONArray();
            return Optional.empty();
        }
    }

    /**
     * [TESTED] The list of processor groups currently instantiated in NiFi
     *
     * @return the list of processor groups in NiFi.
     */
    public List<String> getProcessorGroups() {
        String url = nifiApiUrl + "process-groups/" + getRootProcessGroup() + "/process-groups";
        Optional<JSONObject> response = getNifi(url);
        Optional<JSONArray> groups = response.map(r -> r.getJSONArray("processGroups"));
        return collectIDs(groups.orElse(new JSONArray()));
        // if groups are not null then collectIDs will return the list of ids
        // if groups are null, calls groups with an empty list (and it will return an empty list)
    }

    /**
     * [TESTED only with a print] Get all processors in a processor group
     *
     * @param processGroup a processor group
     * @return the list of processor ids
     */
    public List<String> getProcessors(String processGroup) {
        String url = nifiApiUrl + "process-groups/" + processGroup + "/processors";
        Optional<JSONObject> response = getNifi(url);
        Optional<JSONArray> processes = response.map(r -> r.getJSONArray("processors"));
        return collectIDs(processes.orElse(new JSONArray()));
        // if processes are null, then calls collectIDs with an empty JSONArray and will return an empty list
        // otherwise collects the processors ids
    }

    /**
     * Returns a JSON object with the information of a processor group in the flow
     *
     * @param processorGroupId id of the processor group in the flow
     * @return the json object in an Optional
     */
    public Optional<JSONObject> getProcessorGroupInfo(String processorGroupId) {
        String processorGroup = nifiApiUrl + "flow/process-groups/" + processorGroupId;
        return getNifi(processorGroup);
    }

    /**
     * Removes a processGroup from the flow given its id
     *
     * @param processGroupId the id of the processGroup to remove from the flow
     * @return the response
     */
    public boolean removeProcessGroup(String processGroupId) {
        // To remove a process group we need two parameters: revisionNumber and groupId.
        // The revision number is presumably always 0, but I can be wrong
        String processGroupToRemove = nifiApiUrl + "process-groups/" + processGroupId + "?version=0";
        return deleteNifi(processGroupToRemove, true);
    }

    /**
     * Use only during instantiation!!!
     *
     * @return a list of NifiControllerService objects
     * @param processGroup
     */
    public List<NifiControllerService> getControllerServices(String processGroup) {

        String s = nifiApiUrl + "flow/process-groups/" + processGroup + "/controller-services";
        System.out.println(s);
        List<NifiControllerService> ids = new ArrayList<>();
        Optional<JSONObject> controllerServicesJSON = getNifi(s);
        if (controllerServicesJSON.isPresent()) {
            JSONArray controllerServices = controllerServicesJSON.get().getJSONArray("controllerServices");
            for (int i=0, size=controllerServices.length(); i<size; i++) {
                JSONObject jo = (JSONObject) controllerServices.get(i);
                String state = jo.getJSONObject("component").getString("state");
                ids.add(new NifiControllerService(jo.getString("id"), state, 0));
            }
        }
        return ids;
    }

    public boolean removeControllerService(NifiControllerService ncs) {
        String remove = nifiApiUrl + "controller-services/" + ncs.getId() + "?version=" + ncs.getVersion() + "&disconnectedNodeAcknowledged=false";
        return deleteNifi(remove, false);
    }

    private List<String> collectIDs(JSONArray array) {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < array.length(); i++) {
            JSONObject object = array.getJSONObject(i);
            ids.add(object.getString("id"));
        }
        return ids;
    }

    public boolean runControllerService(NifiControllerService cs) {
        String url = nifiApiUrl + "controller-services/" + cs.getId() + "/run-status";
        String json = "{\n" +
                      "  \"revision\": {\n" +
                      "    \"version\": " + cs.getVersion() + "\n" +
                      "  },\n" +
                      " \"state\": \"ENABLED\", " +
                      "  \"disconnectedNodeAcknowledged\": false,\n" +
                      "  \"uiOnly\": false\n" +
                      "}";
        cs.incrementVersion();
        return putNifi(url, json);
    }

    public boolean stopControllerService(NifiControllerService cd) {
        String url = nifiApiUrl + "controller-services/" + cd.getId() + "/run-status";
        String json = "{\n" +
                      "  \"revision\": {\n" +
                      "    \"version\": " + cd.getVersion() + "\n" +
                      "  },\n" +
                      " \"state\": \"DISABLED\", " +
                      "  \"disconnectedNodeAcknowledged\": false,\n" +
                      "  \"uiOnly\": false\n" +
                      "}";
        cd.incrementVersion();
        return putNifi(url, json);
    }

    /**
     * Used to RUN or STOP an entire processor group
     * @param pgid    process group id
     * @param state   STOPPED or RUNNING
     * @return
     */
    public boolean setRunStatusOfProcessorGroup(String pgid, String state) {
        String s = nifiApiUrl + "flow/process-groups/" + pgid;
        String json = "{\n" +
                      "    \"id\": \"" + pgid + "\",\n" +
                      "    \"state\": \"" + state + "\",\n" +
                      "    \"disconnectedNodeAcknowledged\": true\n" +
                      "}";
        return putNifi(s, json);
    }

    public boolean emptyQueues(String processingGroup) {
        // first we need to get the connection (e.g. the queue id)
        String url = nifiApiUrl + "process-groups/" + processingGroup + "/connections";
        Optional<JSONObject> connJSON = getNifi(url);
        if (connJSON.isPresent()) {
            JSONArray connections = connJSON.get().getJSONArray("connections");
            for (Object o : connections) {
                JSONObject conn = (JSONObject) o;
                String connId = conn.getString("id");
                // then we drop all flow files
                String dropURL = nifiApiUrl + "flowfile-queues/" + connId + "/drop-requests";
                postNifiImportTemplate(dropURL, null);
            }
            return true;
        }
        return false;
    }

    public boolean terminateThreadsOfProcessorGroup(String processorGroupId) {
        List<String> processors = getProcessors(processorGroupId);
        boolean ok = true;
        for (String processor : processors) {
            ok = ok && terminateProcessor(processor);
            System.out.println("Terminated threads of processor " + processor);
        }
        return ok;
    }

    private boolean terminateProcessor(String processorId) {
        String url = nifiApiUrl + "processors/" + processorId + "/threads";
        return deleteNifi(url, false);
    }
}
