package org.elasticsearch.transport.couchbase.capi;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class RegexKeyFilter implements KeyFilter {
    protected ESLogger logger = Loggers.getLogger(getClass());
    private Map<String,String> keyFilterPatternStrings;
    private Map<String, Pattern> keyFilterPatterns;
    private AllowDocumentFilter allowDocumentFilter;

    @Override
    public void configure(Settings settings) {
        String keyFilterType = settings.get("couchbase.keyFilterType", DefaultKeyFilter.DEFAULT_KEY_FILTER_TYPE);
        logger.trace("Using key filter type: {}", keyFilterType);
        this.keyFilterPatterns = new HashMap<String,Pattern>();
        this.keyFilterPatternStrings = settings.getByPrefix("couchbase.keyFilters.").getAsMap();
        for (String key : keyFilterPatternStrings.keySet()) {
            String pattern = keyFilterPatternStrings.get(key);
            logger.trace("See key filter: {} with pattern: {} compiling...", key, pattern);
            keyFilterPatterns.put(key, Pattern.compile(pattern));
        }
        if(keyFilterType.toLowerCase().equals("include")) {
            allowDocumentFilter = new AllowDocumentFilter() {
                @Override
                public boolean allow(String index, String docId) {
                    return  matchesAnyFilter(index, docId);
                }
            };
        } else {
            allowDocumentFilter = new AllowDocumentFilter() {
                @Override
                public boolean allow(String index, String docId) {
                    return !matchesAnyFilter(index, docId);
                }
            };
        }

    }

    @Override
    public Boolean shouldAllow(String index, String docId) {
        return allowDocumentFilter.allow(index,docId);
    }

    private Boolean matchesAnyFilter(String index, String docId) {
        Boolean include = false;

        for(Map.Entry<String,Pattern> typePattern : this.keyFilterPatterns.entrySet()) {
            include = include || typePattern.getValue().matcher(docId).matches();
        }

        return include;
    }

    private static interface AllowDocumentFilter {
        boolean allow(String index, String docId);
    }
}
