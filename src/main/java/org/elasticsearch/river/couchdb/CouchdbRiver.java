/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.river.couchdb;

import static org.elasticsearch.client.Requests.deleteRequest;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Closeables;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.jsr166y.LinkedTransferQueue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;

/**
 *
 */
public class CouchdbRiver extends AbstractRiverComponent implements River {

    private final Client client;

    private final String riverIndexName;

    private final String couchProtocol;
    private final String couchHost;
    private final int couchPort;
    private final String couchDb;
    private final String couchFilter;
    private final String couchFilterParamsUrl;
    private final String basicAuth;
    private final boolean noVerify;
    private final boolean couchIgnoreAttachments;
    private final TimeValue heartbeat;
    private final TimeValue readTimeout;

    private final String indexName;
    private final String typeName;
    private final int bulkSize;
    private final TimeValue bulkTimeout;
    private final int throttleSize;

    private final ExecutableScript script;

    private volatile Thread slurperThread;
    private volatile Thread indexerThread;
    private volatile boolean closed;

    private final String fieldToDermolize;

    private final BlockingQueue<String> stream;

    @SuppressWarnings({"unchecked"})
    @Inject
    public CouchdbRiver(RiverName riverName, RiverSettings settings, @RiverIndexName String riverIndexName, Client client, ScriptService scriptService) {
        super(riverName, settings);
        this.riverIndexName = riverIndexName;
        this.client = client;

        if (settings.settings().containsKey("couchdb")) {
            Map<String, Object> couchSettings = (Map<String, Object>) settings.settings().get("couchdb");
            couchProtocol = XContentMapValues.nodeStringValue(couchSettings.get("protocol"), "http");
            noVerify = XContentMapValues.nodeBooleanValue(couchSettings.get("no_verify"), false);
            couchHost = XContentMapValues.nodeStringValue(couchSettings.get("host"), "localhost");
            couchPort = XContentMapValues.nodeIntegerValue(couchSettings.get("port"), 5984);
            couchDb = XContentMapValues.nodeStringValue(couchSettings.get("db"), riverName.name());
            couchFilter = XContentMapValues.nodeStringValue(couchSettings.get("filter"), null);
            if (couchSettings.containsKey("filter_params")) {
                Map<String, Object> filterParams = (Map<String, Object>) couchSettings.get("filter_params");
                StringBuilder sb = new StringBuilder();
                for (Map.Entry<String, Object> entry : filterParams.entrySet()) {
                    try {
                        sb.append("&").append(URLEncoder.encode(entry.getKey(), "UTF-8")).append("=").append(URLEncoder.encode(entry.getValue().toString(), "UTF-8"));
                    } catch (UnsupportedEncodingException e) {
                        // should not happen...
                    }
                }
                couchFilterParamsUrl = sb.toString();
            } else {
                couchFilterParamsUrl = null;
            }
            heartbeat = XContentMapValues.nodeTimeValue(couchSettings.get("heartbeat"), TimeValue.timeValueSeconds(10));
            readTimeout = XContentMapValues.nodeTimeValue(couchSettings.get("read_timeout"), TimeValue.timeValueSeconds(heartbeat.getSeconds()*3));
            couchIgnoreAttachments = XContentMapValues.nodeBooleanValue(couchSettings.get("ignore_attachments"), false);
            if (couchSettings.containsKey("user") && couchSettings.containsKey("password")) {
                String user = couchSettings.get("user").toString();
                String password = couchSettings.get("password").toString();
                basicAuth = "Basic " + Base64.encodeBytes((user + ":" + password).getBytes());
            } else {
                basicAuth = null;
            }

            if (couchSettings.containsKey("script")) {
                String scriptType = "js";
                if(couchSettings.containsKey("scriptType")) {
                    scriptType = couchSettings.get("scriptType").toString();
                }

                script = scriptService.executable(scriptType, couchSettings.get("script").toString(), Maps.newHashMap());
            } else {
                script = null;
            }
            
            fieldToDermolize = XContentMapValues.nodeStringValue(couchSettings.get("fieldToDermolize"), null);
        } else {
            couchProtocol = "http";
            couchHost = "192.168.0.248";
            couchPort = 5984;
            couchDb = "db_tender_doc";
            couchFilter = null;
            couchFilterParamsUrl = null;
            couchIgnoreAttachments = false;
            heartbeat = TimeValue.timeValueSeconds(10);
            readTimeout = TimeValue.timeValueSeconds(heartbeat.getSeconds()*3);
            noVerify = false;
            basicAuth = null;
            script = null;
            fieldToDermolize = "docs";
        }

        if (settings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
            indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), couchDb);
            typeName = XContentMapValues.nodeStringValue(indexSettings.get("type"), couchDb);
            bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
            if (indexSettings.containsKey("bulk_timeout")) {
                bulkTimeout = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(indexSettings.get("bulk_timeout"), "10ms"), TimeValue.timeValueMillis(10));
            } else {
                bulkTimeout = TimeValue.timeValueMillis(10);
            }
            throttleSize = XContentMapValues.nodeIntegerValue(indexSettings.get("throttle_size"), bulkSize * 5);
        } else {
            indexName = couchDb;
            typeName = couchDb;
            bulkSize = 100;
            bulkTimeout = TimeValue.timeValueMillis(10);
            throttleSize = bulkSize * 5;
        }
        if (throttleSize == -1) {
            stream = new LinkedTransferQueue<String>();
        } else {
            stream = new ArrayBlockingQueue<String>(throttleSize);
        }
    }

    @Override
    public void start() {
        logger.info("starting couchdb stream: host [{}], port [{}], filter [{}], db [{}], indexing to [{}]/[{}]", couchHost, couchPort, couchFilter, couchDb, indexName, typeName);
        try {
            client.admin().indices().prepareCreate(indexName).execute().actionGet();
        } catch (Exception e) {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                // that's fine
            } else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
                // ok, not recovered yet..., lets start indexing and hope we recover by the first bulk
                // TODO: a smarter logic can be to register for cluster event listener here, and only start sampling when the block is removed...
            } else {
                logger.warn("failed to create index [{}], disabling river...", e, indexName);
                return;
            }
        }

        slurperThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "couchdb_river_slurper").newThread(new Slurper());
        indexerThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "couchdb_river_indexer").newThread(new Indexer());
        indexerThread.start();
        slurperThread.start();
        new Timer(true).schedule(new TimerTask() {
			
			@Override
			public void run() {
				logger.info("Total lines: {}. [Index time: {}, lines in ms: {}], [Prapare time: {}, lines in ms: {}], [Net time: {}, lines in ms: {}]", lines_count, index_time/1e6, lines_count/(index_time/1e6), prepare_time/1e6, lines_count/(prepare_time/1e6), net_wait_time/1e6, lines_count/(net_wait_time/1e6));
				logger.info("Slurper data available: {}", available);
			}
		}, 10000, 10000);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        logger.info("closing couchdb stream river");
        slurperThread.interrupt();
        indexerThread.interrupt();
        closed = true;
    }

    @SuppressWarnings({"unchecked"})
    private Object processLine(String s, BulkRequestBuilder bulk) {
    	long start_time = System.nanoTime();
        Map<String, Object> ctx;
        try {
            ctx = XContentFactory.xContent(XContentType.JSON).createParser(s).mapAndClose();
        } catch (IOException e) {
            logger.warn("failed to parse {}", e, s);
            return null;
        }
        if (ctx.containsKey("error")) {
            logger.warn("received error {}", s);
            return null;
        }
        Object seq = ctx.get("seq");
        String id = ctx.get("id").toString();

        // Ignore design documents
        if (id.startsWith("_design/")) {
            if (logger.isTraceEnabled()) {
                logger.trace("ignoring design document {}", id);
            }
            return seq;
        }

        if (script != null) {
            script.setNextVar("ctx", ctx);
            try {
                script.run();
                // we need to unwrap the ctx...
                ctx = (Map<String, Object>) script.unwrap(ctx);
            } catch (Exception e) {
                logger.warn("failed to script process {}, ignoring", e, ctx);
                return seq;
            }
        }

        id = (ctx.get("id") == null) ? null : ctx.get("id").toString();

        if (ctx.containsKey("ignore") && ctx.get("ignore").equals(Boolean.TRUE)) {
            // ignore dock
        } else if (ctx.containsKey("deleted") && ctx.get("deleted").equals(Boolean.TRUE)) {
            String index = extractIndex(ctx);
            String type = extractType(ctx);
            if (logger.isTraceEnabled()) {
                logger.trace("processing [delete]: [{}]/[{}]/[{}]", index, type, id);
            }
            bulk.add(deleteRequest(index).type(type).id(id).routing(extractRouting(ctx)).parent(extractParent(ctx)));
            if (fieldToDermolize != null) {
                Map<String, Object> doc = (Map<String, Object>) ctx.get("doc");
            	if (!doc.containsKey(fieldToDermolize) || !(doc.get(fieldToDermolize) instanceof List<?>)) {
                    logger.warn("field for denormalize is not found or is not List. doc:{}", doc);
                    return null;
            	}
	            for (int i = 0; i < ((List<Map<String, Object>>)doc.get(fieldToDermolize)).size(); i++) {
	                bulk.add(deleteRequest(index).type(type).id(id + "_" + i).parent(id));
				}
            } else {
                bulk.add(deleteRequest(index).type(type).id(id).routing(extractRouting(ctx)).parent(extractParent(ctx)));
            }

        } else if (ctx.containsKey("doc")) {
            String index = extractIndex(ctx);
            String type = extractType(ctx);
            Map<String, Object> doc = (Map<String, Object>) ctx.get("doc");

            // Remove _attachment from doc if needed
            if (couchIgnoreAttachments) {
                // no need to log that we removed it, the doc indexed will be shown without it
                doc.remove("_attachments");
            } else {
                // TODO by now, couchDB river does not really store attachments but only attachments meta infomration
                // So we perhaps need to fully support attachments
            }

            if (logger.isTraceEnabled()) {
                logger.trace("processing [index ]: [{}]/[{}]/[{}], source {}", index, type, id, doc);
            }
            if (fieldToDermolize != null) {
            	if (!doc.containsKey(fieldToDermolize) || !(doc.get(fieldToDermolize) instanceof List<?>)) {
                    logger.warn("field for denormalize is not found or is not List. doc:{}", doc);
                    return null;
            	}
	            int i = 0;
	            for (Map<String, Object> nestDoc : (List<Map<String, Object>>)doc.get(fieldToDermolize)) {
	            	for (java.util.Map.Entry<String, Object> docFld : doc.entrySet()) {
	            		if ("_id".equals(docFld.getKey()) || 
	            				"_rev".equals(docFld.getKey()) || 
	            				docFld.getKey().equals(fieldToDermolize) || 
	            				nestDoc.containsKey(docFld.getKey())) continue;
	            		nestDoc.put(docFld.getKey(), docFld.getValue());
					}
	                bulk.add(indexRequest(index).type(type).id(id + "_" + i).source(nestDoc).routing(extractRouting(ctx)).parent(id));
	                i++;
				}
            } else {
            	bulk.add(indexRequest(index).type(type).id(id).source(doc).routing(extractRouting(ctx)).parent(extractParent(ctx)));
            }
        } else {
            logger.warn("ignoring unknown change {}", s);
        }
        prepare_time += System.nanoTime() - start_time;
        lines_count++;
        return seq;
    }

    private String extractParent(Map<String, Object> ctx) {
        return (String) ctx.get("_parent");
    }

    private String extractRouting(Map<String, Object> ctx) {
        return (String) ctx.get("_routing");
    }

    private String extractType(Map<String, Object> ctx) {
        String type = (String) ctx.get("_type");
        if (type == null) {
            type = typeName;
        }
        return type;
    }

    private String extractIndex(Map<String, Object> ctx) {
        String index = (String) ctx.get("_index");
        if (index == null) {
            index = indexName;
        }
        return index;
    }
	long index_time = 0;
	long prepare_time = 0;
	long lines_count = 0;
	long net_wait_time = 0;
    int available;


    private class Indexer implements Runnable {
        @Override
        public void run() {
            while (true) {
                if (closed) {
                    return;
                }
                String s;
                try {
                    s = stream.take();
                } catch (InterruptedException e) {
                    if (closed) {
                        return;
                    }
                    continue;
                }
                long start_time = System.nanoTime();
                BulkRequestBuilder bulk = client.prepareBulk();
                Object lastSeq = null;
                Object lineSeq = processLine(s, bulk);
                if (lineSeq != null) {
                    lastSeq = lineSeq;
                }

                // spin a bit to see if we can get some more changes
                long start = System.nanoTime();
                try {
                    while ((s = stream.poll(bulkTimeout.millis(), TimeUnit.MILLISECONDS)) != null) {
                    	long diff = System.nanoTime() - start;
                    	net_wait_time += diff;
                    	//logger.info(("indexer wait time: {}"), diff/1e6);
                        lineSeq = processLine(s, bulk);
                        if (lineSeq != null) {
                            lastSeq = lineSeq;
                        }

                        if (bulk.numberOfActions() >= bulkSize) {
                            break;
                        }
                        start = System.nanoTime();
                    }
                } catch (InterruptedException e) {
                    if (closed) {
                        return;
                    }
                }

                if (lastSeq != null) {
                    try {
                        // we always store it as a string
                        String lastSeqAsString = null;
                        if (lastSeq instanceof List) {
                            // bigcouch uses array for the seq
                            try {
                                XContentBuilder builder = XContentFactory.jsonBuilder();
                                //builder.startObject();
                                builder.startArray();
                                for (Object value : ((List) lastSeq)) {
                                    builder.value(value);
                                }
                                builder.endArray();
                                //builder.endObject();
                                lastSeqAsString = builder.string();
                            } catch (Exception e) {
                                logger.error("failed to convert last_seq to a json string", e);
                            }
                        } else {
                            lastSeqAsString = lastSeq.toString();
                        }
                        if (logger.isTraceEnabled()) {
                            logger.trace("processing [_seq  ]: [{}]/[{}]/[{}], last_seq [{}]", riverIndexName, riverName.name(), "_seq", lastSeqAsString);
                        }
                        bulk.add(indexRequest(riverIndexName).type(riverName.name()).id("_seq")
                                .source(jsonBuilder().startObject().startObject("couchdb").field("last_seq", lastSeqAsString).endObject().endObject()));
                    } catch (IOException e) {
                        logger.warn("failed to add last_seq entry to bulk indexing");
                    }
                }

                try {
                	int numberOfActions = bulk.numberOfActions();
                    BulkResponse response = bulk.execute().actionGet();
                    index_time += System.nanoTime() - start_time;
                	//logger.info("Total lines: {}({}). [Index time: {}, lines in ms: {}], [Prapare time: {}, lines in ms: {}], [Net time: {}, lines in ms: {}]", lines_count, numberOfActions, index_time/1e6, lines_count/(index_time/1e6), prepare_time/1e6, lines_count/(prepare_time/1e6), net_wait_time/1e6, lines_count/(net_wait_time/1e6));

                    if (response.hasFailures()) {
                        // TODO write to exception queue?
                        logger.warn("failed to execute" + response.buildFailureMessage());
                    }
                } catch (Exception e) {
                    logger.warn("failed to execute bulk", e);
                }
            }
        }
    }


    private class Slurper implements Runnable {

		@SuppressWarnings({"unchecked"})
        @Override
        public void run() {

            while (true) {
                if (closed) {
                    return;
                }

                String lastSeq = null;
                try {
                    client.admin().indices().prepareRefresh(riverIndexName).execute().actionGet();
                    GetResponse lastSeqGetResponse = client.prepareGet(riverIndexName, riverName().name(), "_seq").execute().actionGet();
                    if (lastSeqGetResponse.isExists()) {
                        Map<String, Object> couchdbState = (Map<String, Object>) lastSeqGetResponse.getSourceAsMap().get("couchdb");
                        if (couchdbState != null) {
                            lastSeq = couchdbState.get("last_seq").toString(); // we know its always a string
                        }
                    }
                } catch (Exception e) {
                    logger.warn("failed to get last_seq, throttling....", e);
                    try {
                        Thread.sleep(5000);
                        continue;
                    } catch (InterruptedException e1) {
                        if (closed) {
                            return;
                        }
                    }
                }

                String file = "/" + couchDb + "/_changes?feed=continuous&include_docs=true&heartbeat=" + heartbeat.getMillis();
                if (couchFilter != null) {
                    try {
                        file = file + "&filter=" + URLEncoder.encode(couchFilter, "UTF-8");
                    } catch (UnsupportedEncodingException e) {
                        // should not happen!
                    }
                    if (couchFilterParamsUrl != null) {
                        file = file + couchFilterParamsUrl;
                    }
                }

                if (lastSeq != null) {
                    try {
                        file = file + "&since=" + URLEncoder.encode(lastSeq, "UTF-8");
                    } catch (UnsupportedEncodingException e) {
                        // should not happen, but in any case...
                        file = file + "&since=" + lastSeq;
                    }
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("using host [{}], port [{}], path [{}]", couchHost, couchPort, file);
                }

                HttpURLConnection connection = null;
                InputStream is = null;
                try {
                    URL url = new URL(couchProtocol, couchHost, couchPort, file);
                    connection = (HttpURLConnection) url.openConnection();
                    if (basicAuth != null) {
                        connection.addRequestProperty("Authorization", basicAuth);
                    }
                    connection.setDoInput(true);
                    connection.setReadTimeout((int) readTimeout.getMillis());
                    connection.setUseCaches(false);

                    if (noVerify) {
                        ((HttpsURLConnection) connection).setHostnameVerifier(
                                new HostnameVerifier() {
                                    public boolean verify(String string, SSLSession ssls) {
                                        return true;
                                    }
                                }
                        );
                    }

                    is = connection.getInputStream();

                    final BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
                    String line;
                    //long start = System.nanoTime();
                    while ((line = reader.readLine()) != null) {
                    	available = is.available();
                    	//if (available>1000)
                    	//logger.info("Slurper data available: {}", available);
                        //long diff = System.nanoTime() - start;
                        //logger.info("Slurper wait time: {}", diff/1e6);
                        if (closed) {
                            return;
                        }
                        if (line.length() == 0) {
                            logger.trace("[couchdb] heartbeat");
                            continue;
                        }
                        if (logger.isTraceEnabled()) {
                            logger.trace("[couchdb] {}", line);
                        }
                        // we put here, so we block if there is no space to add
                        stream.put(line);
                        //start = System.nanoTime();
                    }
                } catch (Exception e) {
                    try {
                        Closeables.close(is, true);
                    } catch (IOException e1) {
                        // Ignore
                    }
                    if (connection != null) {
                        try {
                            connection.disconnect();
                        } catch (Exception e1) {
                            // ignore
                        } finally {
                            connection = null;
                        }
                    }
                    if (closed) {
                        return;
                    }
                    logger.warn("failed to read from _changes, throttling....", e);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e1) {
                        if (closed) {
                            return;
                        }
                    }
                } finally {
                    try {
                        Closeables.close(is, true);
                    } catch (IOException e1) {
                        // Ignore
                    }
                    if (connection != null) {
                        try {
                            connection.disconnect();
                        } catch (Exception e1) {
                            // ignore
                        } finally {
                            connection = null;
                        }
                    }
                }
            }
        }
    }
}
