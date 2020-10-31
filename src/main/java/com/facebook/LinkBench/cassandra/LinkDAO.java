package com.facebook.LinkBench.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.facebook.LinkBench.Link;
import com.facebook.LinkBench.LinkStore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

class LinkDAO {

    private final CqlSession conn;
    private final PreparedStatement getLinksStatement;
    private final PreparedStatement getOutLinksStatement;
    private final PreparedStatement countLinksStatement;
    private final PreparedStatement insertLinkStatement;
    private final PreparedStatement incCountStatement;

    LinkDAO(CqlSession conn, String linkTable, String countTable) {
        this.conn = conn;
        getLinksStatement = conn.prepare(
                "SELECT id2, vy, dt, tm, vn " +
                        "FROM " + linkTable + " " +
                        "WHERE id1=:id1 AND tp=:tp AND id2 IN :id2s"
        );
        getOutLinksStatement = conn.prepare(
                "SELECT id2, vy, dt, tm, vn " +
                        // TODO: dehardcode table name
                        "FROM links_by_time " +
                        "WHERE id1=:id1 AND tp=:tp " +
                        " AND tm >= :minTm AND tm <= :maxTm " +
                        // TODO: move not visible to a separate table to avoid ALLOW FILTERING
                        " AND vy=" + LinkStore.VISIBILITY_DEFAULT + " " +
                        "ORDER BY tp, tm DESC " +
                        "LIMIT :lim " +
                        "ALLOW FILTERING"
        );
        countLinksStatement = conn.prepare(
                "SELECT ct " +
                        "FROM " + countTable + " " +
                        "WHERE id=:id AND tp=:tp"
        );
        insertLinkStatement = conn.prepare(
                "INSERT INTO " + linkTable + " " +
                        "(id1, tp, id2, vy, dt, tm, vn) VALUES (:id1, :tp, :id2, :vy, :dt, :tm, :vn) " +
                        "IF NOT EXISTS"
        );
        incCountStatement = conn.prepare(
                "UPDATE " + countTable + " " +
                        "SET ct = ct + :inc " +
                        "WHERE id=:id AND tp=:tp"
        );
    }

    boolean addLink(Link l) {
        BoundStatement bs = insertLinkStatement.bind()
                .setLong("id1", l.id1)
                .setLong("tp", l.link_type)
                .setLong("id2", l.id2)
                .setByte("vy", l.visibility)
                .setByteBuffer("dt", ByteBuffer.wrap(l.data))
                .setLong("tm", l.time)
                .setInt("vn", l.version);
        ResultSet rs = conn.execute(bs);
        if (rs.wasApplied()) {
            if (l.visibility == LinkStore.VISIBILITY_DEFAULT) {
                // TODO: update counter atomically
                BoundStatement bs2 = incCountStatement.bind()
                        .setLong("id", l.id1)
                        .setLong("tp", l.link_type)
                        .setLong("inc", 1);
                // TODO: update version and time
                conn.execute(bs2);
            }
        } else {
            // TODO: update
            throw new UnsupportedOperationException("updateLink");
        }
        return false;
    }

    boolean deleteLink(long id1, long link_type, long id2, boolean noinverse, boolean expunge) {
        throw new UnsupportedOperationException("deleteLink");
        // TODO
        /*
        // First do a select to check if the link is not there, is there and
        // hidden, or is there and visible;
        // Result could be either NULL, VISIBILITY_HIDDEN or VISIBILITY_DEFAULT.
        // In case of VISIBILITY_DEFAULT, later we need to mark the link as
        // hidden, and update counttable.
        // We lock the row exclusively because we rely on getting the correct
        // value of visible to maintain link counts.  Without the lock,
        // a concurrent transaction could also see the link as visible and
        // we would double-decrement the link count.
        String select = "SELECT visibility" +
                " FROM " + dbid + "." + linktable +
                " WHERE id1 = " + id1 +
                " AND id2 = " + id2 +
                " AND link_type = " + link_type +
                " FOR UPDATE;";

        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace(select);
        }

        ResultSet result = stmt_rw.executeQuery(select);

        int visibility = -1;
        boolean found = false;
        while (result.next()) {
            visibility = result.getInt("visibility");
            found = true;
        }

        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace(String.format("(%d, %d, %d) visibility = %d",
                    id1, link_type, id2, visibility));
        }

        if (!found) {
            // do nothing
        } else if (visibility == VISIBILITY_HIDDEN && !expunge) {
            // do nothing
        } else {
            // Only update count if link is present and visible
            boolean updateCount = (visibility != VISIBILITY_HIDDEN);

            // either delete or mark the link as hidden
            String delete;

            if (!expunge) {
                delete = "UPDATE " + dbid + "." + linktable +
                        " SET visibility = " + VISIBILITY_HIDDEN +
                        " WHERE id1 = " + id1 +
                        " AND id2 = " + id2 +
                        " AND link_type = " + link_type + ";";
            } else {
                delete = "DELETE FROM " + dbid + "." + linktable +
                        " WHERE id1 = " + id1 +
                        " AND id2 = " + id2 +
                        " AND link_type = " + link_type + ";";
            }

            if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
                logger.trace(delete);
            }

            stmt_rw.executeUpdate(delete);

            // update count table
            // * if found (id1, link_type) in count table, set
            //   count = (count == 1) ? 0) we decrease the value of count
            //   column by 1;
            // * otherwise, insert new link with count column = 0
            // The update happens atomically, with the latest count and version
            long currentTime = (new Date()).getTime();
            String update = "INSERT INTO " + dbid + "." + counttable +
                    " (id, link_type, count, time, version) " +
                    "VALUES (" + id1 +
                    ", " + link_type +
                    ", 0" +
                    ", " + currentTime +
                    ", " + 0 + ") " +
                    "ON DUPLICATE KEY UPDATE" +
                    " count = IF (count = 0, 0, count - 1)" +
                    ", time = " + currentTime +
                    ", version = version + 1;";

            if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
                logger.trace(update);
            }

            stmt_rw.executeUpdate(update);
        }

        conn_rw.commit();

        return found;*/
    }

    Link getLink(long id1, long link_type, long id2) {
        Link[] res = multigetLinks(id1, link_type, new long[]{id2});
        if (res == null) {
            return null;
        }
        assert (res.length <= 1);
        return res.length == 0 ? null : res[0];
    }

    Link[] multigetLinks(long id1, long link_type, long[] id2s) {
        ArrayList<Long> id2list = new ArrayList<>(id2s.length);
        for (long id2 : id2s) {
            id2list.add(id2);
        }

        BoundStatement bs = getLinksStatement.bind()
                .setLong("id1", id1)
                .setLong("tp", link_type)
                .set("id2s", id2list, ArrayList.class);
        ResultSet rs = conn.execute(bs);
        List<Row> rows = rs.all();

        Link[] results = new Link[rows.size()];
        for (int i = 0; i < results.length; i++) {
            results[i] = createLinkFromRow(id1, link_type, rows.get(i));
        }

        return results;
    }

    Link[] getOutLinks(long id1, long link_type, long minTimestamp, long maxTimestamp, int offset, int limit) {
        BoundStatement bs = getOutLinksStatement.bind()
                .setLong("id1", id1)
                .setLong("tp", link_type)
                .setLong("minTm", minTimestamp)
                .setLong("maxTm", maxTimestamp)
                // TODO: do we need more efficient offset?
                .setInt("lim", offset + limit);

        ResultSet rs = conn.execute(bs);
        List<Row> rows = rs.all();

        if (rows.size() - offset <= 0) {
            return null;
        }

        Link[] links = new Link[rows.size() - offset];
        for (int i = 0; i < links.length; i++) {
            links[i] = createLinkFromRow(id1, link_type, rows.get(i + offset));
        }
        return links;
    }

    private static Link createLinkFromRow(long id1, long link_type, Row row) {
        return new Link(
                id1,
                link_type,
                row.getLong("id2"),
                row.getByte("vy"),
                Objects.requireNonNull(row.getByteBuffer("dt")).array(),
                row.getInt("vn"),
                row.getLong("tm")
        );
    }

    long countLinks(long id1, long link_type) {
        BoundStatement bs = countLinksStatement.bind()
                .setLong("id", id1)
                .setLong("tp", link_type);
        ResultSet rs = conn.execute(bs);
        Row row = rs.one();
        if (row == null) {
            return 0;
        }
        return row.getLong("ct");
    }
}
