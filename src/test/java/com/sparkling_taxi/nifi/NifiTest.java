package com.sparkling_taxi.nifi;

import com.sparkling_taxi.nifi.NifiExecutor;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class NifiTest {

    private final NifiExecutor executor = new NifiExecutor();
    private static final String EXAMPLE_TEMPLATE = ".nifi/templates/multi_file_preprocessing_group.xml";

    @Test
    public void getRootProcessorGroupTest() {
        String root = executor.getRootProcessGroup();
        assertNotNull(root);
        System.out.println("IDs of processor groups in root: " + root);
    }

    @Test
    public void uploadTemplateTest() {
        Optional<String> templateId = executor.uploadTemplate(EXAMPLE_TEMPLATE);

        if (templateId.isPresent()) {
            Optional<JSONObject> jsonObject2 = executor.deleteTemplate(templateId.get());
            jsonObject2.ifPresent(System.out::println);
        }
    }

    @Test
    public void instantiateTemplateTest() {
        // upload a template
        Optional<String> templateId = executor.uploadTemplate(EXAMPLE_TEMPLATE);
        // if it all goes well
        if (templateId.isPresent()) {
            System.out.println("templateId = " + templateId.get());
            // instantiate a processGroup from the template
            Optional<String> s = executor.instantiateTemplate(templateId.get());
            if (s.isPresent()) {
                System.out.println(s.get());
            } else {
                Optional<JSONObject> jsonObject2 = executor.deleteTemplate(templateId.get());
                jsonObject2.ifPresent(System.out::println);
                fail("TEST: Failed to instantiate the template " + EXAMPLE_TEMPLATE);
            }
            // Delete the template to make the test repeatable (doesn't remove the process group instance)
            Optional<JSONObject> jsonObject2 = executor.deleteTemplate(templateId.get());
            jsonObject2.ifPresent(System.out::println);
        }
    }

    /**
     * Initially there are no processor Groups and check all are empty
     */
    @Test
    public void listProcessorGroupsTest() {
        List<String> processorGroups = executor.getProcessorGroups();
        int prevSize = processorGroups.size();

        // calls the previous test to get at least one template...
        instantiateTemplateTest();
        List<String> processorGroupsNew = executor.getProcessorGroups();
        assertEquals(prevSize + 1, processorGroupsNew.size());
        System.out.println(processorGroupsNew);

        // checks Processors inside the processor Group:
        assertEquals(6, executor.getProcessors(processorGroupsNew.get(0)).size());

        // removes all the templates
        for (String processorGroup : processorGroupsNew) {
            executor.removeProcessGroup(processorGroup);
        }
    }
}
