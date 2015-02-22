package org.elasticsearch.river.json;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;
import org.elasticsearch.river.*;

import java.util.concurrent.TimeUnit;
import java.util.Map;

import static org.elasticsearch.client.Requests.indexRequest;

public class JsonRiver extends AbstractRiverComponent implements River {

    private final Client client;
    private final String riverIndexName;

    public static String RIVER_TYPE = "doc";
    public static int RIVER_MAX_BULK_SIZE = 5000;

    private volatile Thread slurperThread;
    private volatile Thread indexerThread;
    private volatile boolean closed = false;
    private volatile TimeValue riverRefreshInterval;
    private volatile String riverUrl;
    private volatile String riverIndex;
    private volatile String riverType;
    private volatile int riverMaxBulkSize;

    private final TransferQueue<RiverProduct> stream = new LinkedTransferQueue<RiverProduct>();

    @Inject public JsonRiver(RiverName riverName, RiverSettings settings, @RiverIndexName String riverIndexName, Client client) {
        super(riverName, settings);
        this.riverIndexName = riverIndexName;
        this.client = client;

        if (settings.settings().containsKey("json")) {
            Map<String, Object> feed = (Map<String, Object>) settings.settings().get("json");

            riverUrl = XContentMapValues.nodeStringValue(feed.get("url"), null);
            if (riverUrl == null) {
                logger.error("`url` is not set. Please define it.");
                closed = true;
                return;
            }

            riverRefreshInterval = XContentMapValues.nodeTimeValue(feed.get("update_rate"), null);
            if (riverRefreshInterval == null) {
                riverRefreshInterval = TimeValue.timeValueSeconds(30);
                logger.warn("You didn't define the update rate. Switching to defaults : [{}] sec.", riverRefreshInterval);
            }

            riverIndex = XContentMapValues.nodeStringValue(feed.get("index"), riverName.name());
            riverType = XContentMapValues.nodeStringValue(feed.get("type"), RIVER_TYPE);
            riverMaxBulkSize = XContentMapValues.nodeIntegerValue(feed.get("bulk_size"), RIVER_MAX_BULK_SIZE);
        } else {
            logger.error("You didn't define the json url.");
            closed = true;
            return;
        }
    }

    @Override
    public void start() {
        if (closed) {
            logger.info("json stream river is closed. Exiting");
            return;
        }

        logger.info("Starting JSON stream river: url [{}], query interval [{}]", riverUrl, riverRefreshInterval);

        try {
            slurperThread = EsExecutors.daemonThreadFactory("json_river_slurper").newThread(new Slurper());
            slurperThread.start();
            indexerThread = EsExecutors.daemonThreadFactory("json_river_indexer").newThread(new Indexer());
            indexerThread.start();
        } catch (ElasticsearchException e) {
            logger.error("Error starting indexer and slurper. River is not running", e);
            closed = true;
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        logger.info("closing json stream river");
        slurperThread.interrupt();
        indexerThread.interrupt();
        closed = true;
    }

    private class Slurper implements Runnable {

        private final ESLogger logger = ESLoggerFactory.getLogger(this.getClass().getName());

        @Override
        public void run() {
            RiverImporter importer = new RiverImporter(riverUrl, stream);

            while (!closed) {
                logger.debug("Slurper run() started");
                String lastIndexUpdate = getLastUpdatedTimestamp();

                try {
                    RiverProductImport result = importer.executeImport(lastIndexUpdate);

                    if (result.exportTimestamp != null) {
                        storeLastUpdatedTimestamp(result.exportTimestamp);
                    }

                    logger.info("Slurping [{}] documents with timestamp [{}]", result.exportedProductCount, result.exportTimestamp);
                } catch (ElasticsearchException e) {
                    logger.error("Failed to import data from json stream", e);
                }

                try {
                    Thread.sleep(riverRefreshInterval.getMillis());
                } catch (InterruptedException e1) {}
            }
        }

        private void storeLastUpdatedTimestamp(String exportTimestamp) {
            String json = "{ \"lastUpdatedTimestamp\" : \"" + exportTimestamp + "\" }";
            IndexRequest updateTimestampRequest = indexRequest(riverIndexName).type(riverName.name()).id("lastUpdatedTimestamp").source(json);
            client.index(updateTimestampRequest).actionGet();
        }

        private String getLastUpdatedTimestamp() {
            GetResponse lastUpdatedTimestampResponse = client.prepareGet().setIndex(riverIndexName).setType(riverName.name()).setId("lastUpdatedTimestamp").execute().actionGet();
            if (lastUpdatedTimestampResponse.isExists() && lastUpdatedTimestampResponse.getSource().containsKey("lastUpdatedTimestamp")) {
                return lastUpdatedTimestampResponse.getSource().get("lastUpdatedTimestamp").toString();
            }

            return null;
        }
    }


    private class Indexer implements Runnable {
        private final ESLogger logger = ESLoggerFactory.getLogger(this.getClass().getName());
        private int deletedDocuments = 0;
        private int insertedDocuments = 0;
        private BulkRequestBuilder bulk;
        private StopWatch sw;

        @Override
        public void run() {
            while (!closed) {
                logger.debug("Indexer run() started");
                sw = new StopWatch().start();
                deletedDocuments = 0;
                insertedDocuments = 0;

                try {
                    RiverProduct product = stream.take();
                    bulk = client.prepareBulk();
                    do {
                        addProductToBulkRequest(product);
                    } while ((product = stream.poll(250, TimeUnit.MILLISECONDS)) != null && deletedDocuments + insertedDocuments < riverMaxBulkSize);
                } catch (InterruptedException e) {
                    continue;
                } finally {
                    bulk.execute().actionGet();
                }
                logStatistics();
            }
        }

        private void addProductToBulkRequest(RiverProduct riverProduct) {
            if (riverProduct.action == RiverProduct.Action.DELETE) {
                //bulk.add(deleteRequest(riverIndex).type(riverType).id(riverProduct.id));
                logger.info("DELETING {}/{}/{}", riverIndex, riverType, riverProduct.id);
                client.prepareDelete(riverIndex, riverType, riverProduct.id).execute().actionGet();
                deletedDocuments++;
            } else {
                logger.info("INDEXING {}/{}/{}", riverIndex, riverType, riverProduct.id);
                bulk.add(indexRequest(riverIndex).type(riverType).id(riverProduct.id).source(riverProduct.product));
                insertedDocuments++;
            }
        }

        private void logStatistics() {
            long totalDocuments = deletedDocuments + insertedDocuments;
            long totalTimeInSeconds = sw.stop().totalTime().seconds();
            long totalDocumentsPerSecond = (totalTimeInSeconds == 0) ? totalDocuments : totalDocuments / totalTimeInSeconds;
            logger.info("INDEXED {} documents, {} insertions/updates, {} deletions, {} documents per second", totalDocuments, insertedDocuments, deletedDocuments, totalDocumentsPerSecond);
        }
    }
}