/**
 * Copyright (c) 2012 Couchbase, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package org.elasticsearch.transport.couchbase.capi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.hppc.cursors.ObjectCursor;
import org.elasticsearch.common.logging.ESLogger;

import com.couchbase.capi.CouchbaseBehavior;

public class ElasticSearchCouchbaseBehavior implements CouchbaseBehavior {

    protected Client client;
    protected ESLogger logger;
    protected String checkpointDocumentType;
    protected Cache<String, String> bucketUUIDCache;

    public ElasticSearchCouchbaseBehavior(Client client, ESLogger logger, String checkpointDocumentType, Cache<String, String> bucketUUIDCache) {
        this.client = client;
        this.logger = logger;
        this.checkpointDocumentType = checkpointDocumentType;
        this.bucketUUIDCache = bucketUUIDCache;
    }

    @Override
    public List<String> getPools() {
        List<String> result = new ArrayList<String>();
        result.add("default");
        return result;
    }

    @Override
    public String getPoolUUID(String pool) {
        ClusterStateRequestBuilder builder = client.admin().cluster().prepareState();
        ClusterStateResponse response = builder.execute().actionGet();
        ClusterName name = response.getClusterName();
        return UUID.nameUUIDFromBytes(name.toString().getBytes()).toString().replace("-", "");
    }

    @Override
    public Map<String, Object> getPoolDetails(String pool) {
        if("default".equals(pool)) {
            Map<String, Object> bucket = new HashMap<String, Object>();
            bucket.put("uri", "/pools/" + pool + "/buckets?uuid=" + getPoolUUID(pool));

            Map<String, Object> responseMap = new HashMap<String, Object>();
            responseMap.put("buckets", bucket);

            List<Object> nodes = getNodesServingPool(pool);
            responseMap.put("nodes", nodes);

            return responseMap;
        }
        return null;
    }

    @Override
    public List<String> getBucketsInPool(String pool) {
        if("default".equals(pool)) {
            List<String> bucketNameList = new ArrayList<String>();

            ClusterStateRequestBuilder stateBuilder = client.admin().cluster().prepareState();
            ClusterStateResponse response = stateBuilder.execute().actionGet();
            ImmutableOpenMap<String, IndexMetaData> indices = response.getState().getMetaData().getIndices();
            for (ObjectCursor<String> index : indices.keys()) {
                bucketNameList.add(index.value);
                IndexMetaData indexMetaData = indices.get(index.value);
                ImmutableOpenMap<String, AliasMetaData> aliases = indexMetaData.aliases();
                for(ObjectCursor<String> alias : aliases.keys()) {
                    bucketNameList.add(alias.value);
                }
            }

            return bucketNameList;
        }
        return null;
    }

    protected String getUUIDFromCheckpointDocSource(Map<String, Object> source) {
        Map<String,Object> docMap = (Map<String,Object>)source.get("doc");
        String uuid = (String)docMap.get("uuid");
        return uuid;
    }

    protected String lookupUUID(String bucket, String id) {
        GetRequestBuilder builder = client.prepareGet();
        builder.setIndex(bucket);
        builder.setId(id);
        builder.setType(this.checkpointDocumentType);
        builder.setFetchSource(true);

        String bucketUUID = null;
        GetResponse response;
        ListenableActionFuture<GetResponse> laf = builder.execute();
        if(laf != null) {
            response = laf.actionGet();
            if(response.isExists()) {
            Map<String,Object> responseMap = response.getSourceAsMap();
            bucketUUID = this.getUUIDFromCheckpointDocSource(responseMap);
            }
        }

        return bucketUUID;
    }

    protected void storeUUID(String bucket, String id, String uuid) {
        Map<String,Object> doc = new HashMap<String, Object>();
        doc.put("uuid", uuid);
        Map<String, Object> toBeIndexed = new HashMap<String, Object>();
        toBeIndexed.put("doc", doc);

        IndexRequestBuilder builder = client.prepareIndex();
        builder.setIndex(bucket);
        builder.setId(id);
        builder.setType(this.checkpointDocumentType);
        builder.setSource(toBeIndexed);
        builder.setOpType(OpType.CREATE);

        IndexResponse response;
        ListenableActionFuture<IndexResponse> laf = builder.execute();
        if(laf != null) {
            response = laf.actionGet();
            if(!response.isCreated()) {
                logger.error("did not succeed creating uuid");
            }
        }
    }

    @Override
    public String getBucketUUID(String pool, String bucket) {
        // first look for bucket UUID in cache
        String bucketUUID = this.bucketUUIDCache.getIfPresent(bucket);
        if (bucketUUID != null) {
            logger.debug("found bucket UUID {} in cache", bucketUUID);
            return bucketUUID;
        }

        logger.debug("bucket UUID not in cache, looking up");
        IndicesExistsRequestBuilder existsBuilder = client.admin().indices().prepareExists(bucket);
        IndicesExistsResponse response = existsBuilder.execute().actionGet();
        if(response.isExists()) {
            int tries = 0;
            bucketUUID = this.lookupUUID(bucket, "bucketUUID");
            while(bucketUUID == null && tries < 100) {
                logger.debug("bucket UUID doesn't exist yet, creaating, attempt: {}", tries+1);
                String newUUID = UUID.randomUUID().toString().replace("-", "");
                storeUUID(bucket, "bucketUUID", newUUID);
                bucketUUID = this.lookupUUID(bucket, "bucketUUID");
                tries++;
            }

            if(bucketUUID != null) {
                // store it in the cache
                bucketUUIDCache.put(bucket, bucketUUID);
                return bucketUUID;
            }
        }
        throw new RuntimeException("failed to find/create bucket uuid");
    }

    @Override
    public List<Object> getNodesServingPool(String pool) {
        if("default".equals(pool)) {

            NodesInfoRequestBuilder infoBuilder = client.admin().cluster().prepareNodesInfo((String[]) null);
            NodesInfoResponse infoResponse = infoBuilder.execute().actionGet();

            // extract what we need from this response
            List<Object> nodes = new ArrayList<Object>();
            for (NodeInfo nodeInfo : infoResponse.getNodes()) {

                // FIXME there has to be a better way than
                // parsing this string
                // but so far I have not found it
                if (nodeInfo.getServiceAttributes() != null) {
                    for (Map.Entry<String, String> nodeAttribute : nodeInfo
                            .getServiceAttributes().entrySet()) {
                        if (nodeAttribute.getKey().equals(
                                "couchbase_address")) {
                            int start = nodeAttribute
                                    .getValue()
                                    .lastIndexOf("/");
                            int end = nodeAttribute
                                    .getValue()
                                    .lastIndexOf("]");
                            String hostPort = nodeAttribute
                                    .getValue().substring(
                                            start + 1, end);
                            String[] parts = hostPort.split(":");

                            Map<String, Object> nodePorts = new HashMap<String, Object>();
                            nodePorts.put("direct", Integer.parseInt(parts[1]));

                            Map<String, Object> node = new HashMap<String, Object>();
                            node.put("couchApiBase", String.format("http://%s/", hostPort));
                            node.put("hostname", hostPort);
                            node.put("ports", nodePorts);

                            nodes.add(node);
                        }
                    }
                }
            }
            return nodes;

        }
        return null;
    }

    @Override
    public Map<String, Object> getStats() {
        Map<String, Object> result = new HashMap<String, Object>();
        return result;
    }

}
