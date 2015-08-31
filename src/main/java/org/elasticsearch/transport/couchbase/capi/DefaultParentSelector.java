package org.elasticsearch.transport.couchbase.capi;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;

/**
 * Get parent document Id according to field within document json
 *
 * @author tal.maayani on 1/22/2015.
 */
public class DefaultParentSelector implements IParentSelector {
    protected ESLogger logger;

    private ImmutableMap<String, String> documentTypeParentFields;

    @Override
    public void configure(Settings settings) {
        this.logger = Loggers.getLogger(this.getClass(), settings, new String[0]);

        this.documentTypeParentFields = settings.getByPrefix("couchbase.documentTypeParentFields.").getAsMap();
        if (documentTypeParentFields.isEmpty()) {
            documentTypeParentFields = null;
            return;
        }
        for (String key : documentTypeParentFields.keySet()) {
            String parentField = documentTypeParentFields.get(key);
            logger.info("Using field {} as parent for type {}", parentField, key);
        }
    }

    @Override
    public Object getParent(Map<String, Object> doc, String docId, String type) {
        if (documentTypeParentFields == null) return null;
        String parentField = null;
        if (documentTypeParentFields.containsKey(type)) {
            parentField = documentTypeParentFields.get(type);
        }
        if (parentField == null) return null;
        return JSONMapPath(doc, parentField);
    }

    private Object JSONMapPath(Map<String, Object> json, String path) {
        int dotIndex = path.indexOf('.');
        if (dotIndex >= 0) {
            String pathThisLevel = path.substring(0, dotIndex);
            Object current = json.get(pathThisLevel);
            String pathRest = path.substring(dotIndex + 1);
            if (pathRest.length() == 0) {
                return current;
            } else if (current instanceof Map && pathRest.length() > 0) {
                return JSONMapPath((Map<String, Object>) current, pathRest);
            }
        } else {
            // no dot
            Object current = json.get(path);
            return current;
        }
        return null;
    }
}
