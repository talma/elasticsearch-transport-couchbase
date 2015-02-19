package org.elasticsearch.transport.couchbase.capi;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;

/**
 * Get parent document Id according to field within document json
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
        for (String key: documentTypeParentFields.keySet()) {
            String parentField = documentTypeParentFields.get(key);
            logger.info("Using field {} as parent for type {}", parentField, key);
        }
    }

    @Override
    public Object getParent(Map<String, Object> doc, String docId, String type) {
        String parentField = null;
        if(documentTypeParentFields != null && documentTypeParentFields.containsKey(type)) {
            parentField = documentTypeParentFields.get(type);
        }
        if (parentField == null) return null;
        if(documentTypeParentFields != null && documentTypeParentFields.containsKey(type)) {
            parentField = documentTypeParentFields.get(type);
        }
        return ElasticSearchCAPIBehavior.JSONMapPath(doc, parentField);
    }
}
