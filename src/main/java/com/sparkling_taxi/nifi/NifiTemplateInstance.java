package com.sparkling_taxi.nifi;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

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
        if (executor.getProcessorGroups().isEmpty()) {
            return 0;
        }
        Optional<JSONObject> info = executor.getProcessorGroupInfo(processorGroupId);
        if (info.isPresent()) {
            JSONArray jsonArray = info.get().getJSONObject("processGroupFlow")
                    .getJSONObject("flow")
                    .getJSONArray("processors");
            return (int) IntStream.range(0, jsonArray.length())
                    .mapToObj(jsonArray::getJSONObject)
                    .map(processor -> processor.getJSONObject("component"))
                    .map(c -> c.getString("state"))
                    .filter(s -> s.equals("RUNNING"))
                    .count();
        }
        return 0;
    }

    public int numberControllerServicesRunning() {
        List<NifiControllerService> info = executor.getControllerServices(processorGroupId);
        return (int) info.stream().filter(n -> n.getState().equals("ENABLED")).count();
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
        if (!executor.removeAllTemplates()) {
            System.out.println("Impossible to remove templates");
            return false;
        }
        // upload the template
        System.out.println("Uploading template " + this.templateFile);
        Optional<String> templateId = executor.uploadTemplate(templateFile);
        // if it all goes well
        if (templateId.isPresent()) {
            this.templateId = templateId.get();
            System.out.println("templateId = " + this.templateId);
            // instantiate a processGroup from the template
            System.out.println("Instantiating template " + this.templateFile);
            Optional<String> s = executor.instantiateTemplate(templateId.get());
            if (s.isPresent()) {
                System.out.println("processorGroupId = " + s.get());
                processorGroupId = s.get();
                System.out.println("Getting controller services");
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
        waitUntilAllControllerServicesRunning();
        boolean running = executor.setRunStatusOfProcessorGroup(processorGroupId, "RUNNING");
        waitUntilAllProcessRunning();
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
        waitUntilAllProcessStopped();
        boolean stoppedServices = stopAllControllerServices();
        waitUntilAllControllerServicesStopped();
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
        waitABit("Waiting before removing"); // wait for the queue to empty
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
        for (NifiControllerService ncs : controllerServiceIds) {
            System.out.println("Running controller service: " + ncs.getId());
            ss = ss && executor.runControllerService(ncs);
        }
        return ss;
    }

    public boolean stopAllControllerServices() {
        boolean ss = true;
        for (NifiControllerService ncs : controllerServiceIds) {
            System.out.println("Stopping controller service: " + ncs.getId());
            ss = ss && executor.stopControllerService(ncs);
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

    private void waitABit(String message) {
        try {
            Thread.sleep(500);
            System.out.println(message);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void waitUntilAllProcessRunning() {
        int totalProcessors = getProcessorGroup().map(executor::getProcessors).map(List::size).orElse(0);
        int n;
        do {
            n = numberProcessRunning();
            if (n != totalProcessors) {
                waitABit("Waiting until all " + n + "/" + totalProcessors + " processors are running...");
            }
        } while (n != totalProcessors);
    }

    private void waitUntilAllProcessStopped() {
        int n;
        do {
            n = numberProcessRunning();
            if (n != 0) {
                waitABit("Waiting until all processors are stopped...");
            }
        } while (n != 0);
    }

    private void waitUntilAllControllerServicesRunning() {
        int totalControllerServices = getProcessorGroup().map(executor::getControllerServices).map(List::size).orElse(0);
        int n;
        do {
            n = numberControllerServicesRunning();
            if (n != totalControllerServices) {
                waitABit("Waiting until all controller services are enabled...");
            }
        } while (n != totalControllerServices);
    }

    private void waitUntilAllControllerServicesStopped() {
        int n;
        do {
            n = numberControllerServicesRunning();
            if (n != 0) {
                waitABit("Waiting until all controller services are disabled...");
            }
        } while (n != 0);
    }
}
