package com.sparkling_taxi;

import org.json.JSONObject;
import org.junit.jupiter.api.Order;
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
    public void instantiateTemplateTest(){
        Optional<String> templateId = executor.uploadTemplate(EXAMPLE_TEMPLATE);


        if (templateId.isPresent()) {
            Optional<String> s = executor.instantiateTemplate(templateId.get());
            s.ifPresent(System.out::println);

            Optional<JSONObject> jsonObject2 = executor.deleteTemplate(templateId.get());
            jsonObject2.ifPresent(System.out::println);
        }
    }

    public static void main(String[] args) {
        /* supponendo di aver già caricato il template in nifi */
        // recupera il process group istanziato
        NifiExecutor executor = new NifiExecutor();
        String group = executor.getRootProcessGroup();
        // executor.uploadTemplate(); // Questo template istanzia contiene un process group

        // cerco i process group
        List<String> groups = executor.getProcessorGroups();
        // prendo il primo group, quello presente nel template
        executor.instantiateTemplate(groups.get(0));
        // stamp tutti i gruppi presenti (dovrebbe essere 1)
        System.out.println(groups);
        for (String g : groups) {
            // stampa tutti i processor nel gruppo
            System.out.println(executor.getProcessors(g));
        }
    }
}
