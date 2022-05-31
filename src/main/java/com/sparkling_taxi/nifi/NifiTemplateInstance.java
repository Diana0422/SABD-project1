package com.sparkling_taxi.nifi;

import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Utility class to get a template running
 */
public class NifiTemplateInstance {
    private final String templateFile;

    private final NifiExecutor executor;

    private String templateId;
    private String processorGroupId;

    private int processorGroupVersion;

    private List<NifiControllerService> controllerServiceIds;

    public NifiTemplateInstance(String file, String apiUrl) {
        this.templateFile = file;
        this.executor = new NifiExecutor(apiUrl);
        controllerServiceIds = new ArrayList<>();
    }

    public NifiExecutor getExecutor() {
        return executor;
    }

    public int getProcessorGroupVersion() {
        return processorGroupVersion;
    }

    public void incrementProcessGroupVersion() {
        processorGroupVersion++;
    }

    public int numberProcessRunning() {
        Optional<JSONObject> info = executor.getProcessorGroupInfo(processorGroupId);
        return info.map(jsonObject -> jsonObject.getJSONObject("processGroupFlow"))
                .map(j -> j.getJSONObject("flow"))
                .map(j -> j.getJSONArray("processGroups"))
                .filter(a -> !a.isEmpty())
                .map(a -> a.getJSONObject(0))
                .map(p -> p.getInt("runningCount"))
                .orElse(0);
    }

    /**
     * This method:
     * - upload a  template
     * - instantiate the template
     *
     * @return true if all goes well
     */
    public boolean uploadAndInstantiateTemplate() {
        // preemptively remove all template to avoid conflicts...
        if(!executor.removeAllTemplates()){
            System.out.println("Impossible to remove templates");
            return false;
        }
        // upload the template
        Optional<String> templateId = executor.uploadTemplate(templateFile);
        // if it all goes well
        if (templateId.isPresent()) {
            this.templateId = templateId.get();
            System.out.println("templateId = " + this.templateId);
            // instantiate a processGroup from the template
            Optional<String> s = executor.instantiateTemplate(templateId.get());
            if (s.isPresent()) {
                System.out.println("processorGroupId = " + s.get());
                processorGroupId = s.get();
                controllerServiceIds = executor.getControllerServices(processorGroupId);
                return true;
            }
        }
        return false;
    }

    /**
     * This method:
     * - activate controller services
     * - run all processors
     */
    public boolean runAll() {
        runAllControllerServices();
        waitABit();
        waitABit();
        boolean running = executor.setRunStatusOfProcessorGroup(processorGroupId, "RUNNING");
        incrementProcessGroupVersion();
        return running;
    }

    /**
     * This method:
     * - stop controller services
     * - stop all processors
     * - terminates all running threads
     * - empties all queues
     */
    public boolean stopAll() {
        boolean stoppedProcessGroups = executor.setRunStatusOfProcessorGroup(processorGroupId, "STOPPED");
        waitABit();
        boolean stoppedServices = stopAllControllerServices();
        waitABit();
        boolean terminatedThreads = executor.terminateThreadsOfProcessorGroup(processorGroupId);
        incrementProcessGroupVersion();
        boolean emptied = executor.emptyQueues(processorGroupId);
        return terminatedThreads && stoppedServices && stoppedProcessGroups && emptied;
    }

    /**
     * This method
     * - remove processor group instance
     * - remove controller services instances
     * - delete template from Nifi
     *
     * @return true if all goes well
     */
    public boolean removeAll() {
        waitABit();
        boolean templateDeleted = executor.deleteTemplate(templateId);
        // remove the process group of the template
        List<String> theProcessGroup = executor.getProcessorGroups();
        boolean emptied = executor.emptyQueues(processorGroupId);
        // remove all processGroups
        boolean allGroupsDeleted = theProcessGroup.stream().allMatch(executor::removeProcessGroup);
        System.out.println("removed process groups: " + theProcessGroup.size());
        return templateDeleted && emptied && allGroupsDeleted;
    }

    public List<NifiControllerService> getControllerServices() {
        return controllerServiceIds;
    }

    public boolean runAllControllerServices() {
        boolean ss = true;
        for (NifiControllerService id : controllerServiceIds) {
            ss = ss && executor.runControllerService(id);
            System.out.println("Eseguito running: "+id.getId());
        }
        return ss;
    }

    public boolean stopAllControllerServices() {
        boolean ss = true;
        for (NifiControllerService id : controllerServiceIds) {
            ss = ss && executor.stopControllerService(id);
        }
        return ss;
    }

    /**
     * Gets the processor Group of the instantiated Template.
     * If it is not instantiated return null
     *
     * @return the id of the processor group or null
     */
    public Optional<String> getProcessorGroup() {
        return Optional.ofNullable(processorGroupId);
    }

    private void waitABit(){
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
