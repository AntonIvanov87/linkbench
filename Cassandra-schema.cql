CREATE KEYSPACE linkdb WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};
USE linkdb;

CREATE TABLE nodetable (
   id bigint PRIMARY KEY,
   tp int,
   vn bigint,
   tm int,
   dt blob,
);

CREATE TABLE linktable (
    id1 bigint,
    tp bigint,
    id2 bigint,
    vy tinyint,
    dt blob,
    tm bigint,
    vn int,
    PRIMARY KEY(id1, tp, id2)
);

CREATE MATERIALIZED VIEW links_by_time AS
    SELECT id1, tp, tm, id2, vy, vn, dt FROM linktable
    WHERE (id1 IS NOT NULL) AND (tp IS NOT NULL) AND (id2 IS NOT NULL) AND (tm IS NOT NULL)
    PRIMARY KEY (id1, tp, tm, id2)
    WITH CLUSTERING ORDER BY (tm desc);

CREATE TABLE counttable (
    id bigint,
    tp bigint,
    ct counter,
    PRIMARY KEY(id, tp)
);
