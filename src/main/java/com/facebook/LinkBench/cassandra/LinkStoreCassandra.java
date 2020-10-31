/*
 * Copyright 2012, Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.LinkBench.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.facebook.LinkBench.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class LinkStoreCassandra extends GraphStore {

    /* Cassandra server configuration keys */
    public static final String CONFIG_HOST = "host";
    public static final String CONFIG_PORT = "port";

    private String linktable;
    private String counttable;
    private String nodetable;

    private String host;
    private String port;
    private String keyspace;

    private Level debuglevel;

    private CqlSession conn;
    private NodeDAO nodeDAO;
    private LinkDAO linkDAO;

    private final Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);

    public LinkStoreCassandra() {
        super();
    }

    public LinkStoreCassandra(Properties props) {
        super();
        initialize(props, Phase.LOAD, 0);
    }

    public void initialize(Properties props, Phase currentPhase, int threadId) {
        counttable = ConfigUtil.getPropertyRequired(props, Config.COUNT_TABLE);
        if (counttable.equals("")) {
            String msg = "Error! " + Config.COUNT_TABLE + " is empty!"
                    + "Please check configuration file.";
            throw new RuntimeException(msg);
        }

        nodetable = props.getProperty(Config.NODE_TABLE);
        if (nodetable.equals("")) {
            // For now, don't assume that nodetable is provided
            String msg = "Error! " + Config.NODE_TABLE + " is empty!"
                    + "Please check configuration file.";
            throw new RuntimeException(msg);
        }

        linktable = ConfigUtil.getPropertyRequired(props, Config.LINK_TABLE);

        host = ConfigUtil.getPropertyRequired(props, CONFIG_HOST);

        port = props.getProperty(CONFIG_PORT, "9042");

        keyspace = ConfigUtil.getPropertyRequired(props, Config.DBID);

        debuglevel = ConfigUtil.getDebugLevel(props);

        openConnection();
    }

    private void openConnection() {
        CqlSessionBuilder cqlSessionBuilder = CqlSession.builder();
                // TODO: dehardcode dc
                // .withLocalDatacenter("dc1")
                // TODO: why it does not work?
                // .addContactPoint(new InetSocketAddress(host, Integer.parseInt(port)));

        if (keyspace != null) {
            cqlSessionBuilder.withKeyspace(keyspace);
        }

        // Fix for failing connections at high concurrency, short random delay for each
        try {
            Thread.sleep(ThreadLocalRandom.current().nextInt(1000));
        } catch (InterruptedException ie) {
            throw new RuntimeException("Interrupted while sleeping before opening a connection", ie);
        }

        // TODO: user, pass
        conn = cqlSessionBuilder.build();
        logger.info("Connected to " + conn);

        nodeDAO = new NodeDAO(conn, nodetable);
        linkDAO = new LinkDAO(conn, linktable, counttable);

        // TODO:
        // if (phase == Phase.LOAD && disableBinLogForLoad) {
        // Turn binary logging off for duration of connection
        // stmt_rw.executeUpdate("SET SESSION sql_log_bin=0");
        // stmt_ro.executeUpdate("SET SESSION sql_log_bin=0");
        // }
    }

    @Override
    public void close() {
        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    public void clearErrors(int threadID) {
        // logger.info("Reopening Cassandra connection in threadID " + threadID);
        //
        // try {
        //     if (conn != null) {
        //         conn.close();
        //     }
        //
        //     openConnection();
        // } catch (Throwable e) {
        //     e.printStackTrace();
        // }
    }

    @Override
    public boolean addLink(String dbid, Link l, boolean noinverse) {
        checkKeyspace(dbid);
        return linkDAO.addLink(l);
    }

    @Override
    public boolean deleteLink(String dbid, long id1, long link_type, long id2,
                              boolean noinverse, boolean expunge) {
        checkKeyspace(dbid);
        return linkDAO.deleteLink(id1, link_type, id2, noinverse, expunge);
    }

    @Override
    public boolean updateLink(String dbid, Link l, boolean noinverse) {
        checkKeyspace(dbid);
        // Retry logic is in addLink
        boolean added = addLink(dbid, l, noinverse);
        return !added; // return true if updated instead of added
    }

    @Override
    public Link getLink(String dbid, long id1, long link_type, long id2) {
        checkKeyspace(dbid);
        return linkDAO.getLink(id1, link_type, id2);
    }

    @Override
    public Link[] multigetLinks(String dbid, long id1, long link_type, long[] id2s) {
        checkKeyspace(dbid);
        return linkDAO.multigetLinks(id1, link_type, id2s);
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type) {
        return getLinkList(dbid, id1, link_type, 0, Long.MAX_VALUE, 0, rangeLimit);
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type,
                              long minTimestamp, long maxTimestamp,
                              int offset, int limit) {
        checkKeyspace(dbid);
        return linkDAO.getOutLinks(id1, link_type, minTimestamp, maxTimestamp, offset, limit);
    }

    @Override
    public long countLinks(String dbid, long id1, long link_type) {
        checkKeyspace(dbid);
        return linkDAO.countLinks(id1, link_type);
    }

    @Override
    public int bulkLoadBatchSize() {
        return 0;
    }

    @Override
    public void addBulkLinks(String dbid, List<Link> links, boolean noinverse) {
        throw new UnsupportedOperationException("LinkStoreCassandra.addBulkLinks");
    }

    @Override
    public void addBulkCounts(String dbid, List<LinkCount> counts) {
        throw new UnsupportedOperationException("LinkStoreCassandra.addBulkCounts");
    }

    @Override
    public void resetNodeStore(String dbid, long startID) {
        checkKeyspace(dbid);
        nodeDAO.resetNodeStore();
    }

    @Override
    public long addNode(String dbid, Node node) {
        checkKeyspace(dbid);
        return nodeDAO.addNode(node);
    }

    @Override
    public long[] bulkAddNodes(String dbid, List<Node> nodes) {
        checkKeyspace(dbid);
        return nodeDAO.bulkAddNodes(nodes);
    }

    @Override
    public Node getNode(String dbid, int type, long id) {
        checkKeyspace(dbid);
        return nodeDAO.getNode(type, id);
    }

    @Override
    public boolean updateNode(String dbid, Node node) {
        checkKeyspace(dbid);
        return nodeDAO.updateNode(node);
    }

    @Override
    public boolean deleteNode(String dbid, int type, long id) {
        checkKeyspace(dbid);
        return nodeDAO.deleteNode(type, id);
    }

    private void checkKeyspace(String dbid) {
        if (!dbid.equals(keyspace)) {
            throw new UnsupportedOperationException(dbid + " is not equal to the default keyspace " + keyspace);
        }
    }
}
