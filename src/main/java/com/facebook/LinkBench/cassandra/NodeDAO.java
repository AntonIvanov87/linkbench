package com.facebook.LinkBench.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.facebook.LinkBench.Node;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

class NodeDAO {

    // TODO: read maxId from DB
    private static final AtomicLong nodeIdGenerator = new AtomicLong(1);

    private final CqlSession conn;
    private final String table;
    private final PreparedStatement insertStatement;
    private final PreparedStatement getStatement;
    private final PreparedStatement updateStatement;
    private final PreparedStatement deleteStatement;

    NodeDAO(CqlSession conn, String table) {
        this.table = table;
        this.conn = conn;

        insertStatement = conn.prepare(
                "INSERT INTO " + table + " " +
                        "(id, tp, vn, tm, dt) VALUES (:id, :tp, :vn, :tm, :dt)");
        getStatement = conn.prepare(
                "SELECT tp, vn, tm, dt " +
                        "FROM " + table + " " +
                        "WHERE id=:id");
        updateStatement = conn.prepare(
                "UPDATE " + table + " " +
                        "SET vn=:vn, tm=:tm, dt=:dt " +
                        "WHERE id=:id " +
                        "IF tp=:tp"
        );
        deleteStatement = conn.prepare(
                "DELETE FROM " + table + " " +
                        "WHERE id=:id " +
                        "IF tp=:tp"
        );
    }

    void resetNodeStore() {
        conn.execute("TRUNCATE TABLE " + table + "");
        // TODO:
        // stmt_rw.execute(String.format("ALTER TABLE `%s`.`%s` " +
        //         "AUTO_INCREMENT = %d;", dbid, nodetable, startID));
    }

    long addNode(Node node) {
        long[] ids = bulkAddNodes(Collections.singletonList(node));
        assert (ids.length == 1);
        return ids[0];
    }

    long[] bulkAddNodes(List<Node> nodes) {
        long[] newIds = new long[nodes.size()];
        for (int i = 0; i < newIds.length; i++) {
            long newId = nodeIdGenerator.getAndIncrement();

            Node node = nodes.get(i);
            BoundStatement bs = insertStatement.bind()
                    .setLong("id", newId)
                    .setInt("tp", node.type)
                    .setLong("vn", node.version)
                    .setInt("tm", node.time)
                    .setByteBuffer("dt", ByteBuffer.wrap(node.data));
            conn.execute(bs);

            newIds[i] = newId;
        }

        return newIds;
    }

    Node getNode(int type, long id) {
        BoundStatement bs = getStatement.bind().setLong("id", id);
        ResultSet rs = conn.execute(bs);
        Row r = rs.one();

        if (r == null) {
            return null;
        }
        if (r.getInt("tp") != type) {
            return null;
        }

        return new Node(
                id,
                type,
                r.getLong("vn"),
                r.getInt("tm"),
                Objects.requireNonNull(r.getByteBuffer("dt")).array()
        );
    }

    boolean updateNode(Node node) {
        BoundStatement bs = updateStatement.bind()
                .setLong("id", node.id)
                .setInt("tp", node.type)
                .setLong("vn", node.version)
                .setInt("tm", node.time)
                .setByteBuffer("dt", ByteBuffer.wrap(node.data));
        ResultSet rs = conn.execute(bs);
        return rs.wasApplied();
    }

    boolean deleteNode(int type, long id) {
        BoundStatement bs = deleteStatement.bind()
                .setLong("id", id)
                .setInt("tp", type);
        ResultSet rs = conn.execute(bs);
        return rs.wasApplied();
    }
}
