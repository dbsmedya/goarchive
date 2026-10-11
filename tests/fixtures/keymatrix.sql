-- The key-matrix fixture (spec §10): tables whose keys Sakila does not have, loaded
-- into the source `sakila` database after Sakila itself, by lib/estate.sh. The
-- destination's copies come from the same schema dump as Sakila's.
--
-- Each shape is the root of one E2E test; the full cross-product of key types and
-- job types runs in the integration suite (internal/engine, TestMySQLKeyMatrix).
--
--   km_line      composite root (order_id, line_no), a child on the composite
--                key, and a grandchild on a three-column composite FK   test 14, archive
--   km_blob      BINARY(16) root whose bytes are not valid UTF-8          test 15, purge
--   km_name      latin1 VARCHAR root with non-ASCII values               test 16, copy-only
--
-- Every row count below is fixed by the generators, and each test's expected_rows
-- was measured against them.

DROP TABLE IF EXISTS km_item_note, km_item, km_line, km_blob_ref, km_blob, km_name_tag, km_name;

-- 100 orders x 3 lines. The tests select order_id <= 60: 180 lines.
CREATE TABLE km_line (
  order_id INT NOT NULL,
  line_no SMALLINT NOT NULL,
  sku VARCHAR(16) NOT NULL,
  PRIMARY KEY (order_id, line_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 2 items per line, keyed on the line's key plus item_no.
CREATE TABLE km_item (
  order_id INT NOT NULL,
  line_no SMALLINT NOT NULL,
  item_no TINYINT NOT NULL,
  qty INT NOT NULL,
  PRIMARY KEY (order_id, line_no, item_no),
  CONSTRAINT fk_km_item_line FOREIGN KEY (order_id, line_no) REFERENCES km_line (order_id, line_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 1 note per item, on its own key, referencing the item's three-column key.
CREATE TABLE km_item_note (
  note_id INT NOT NULL,
  order_id INT NOT NULL,
  line_no SMALLINT NOT NULL,
  item_no TINYINT NOT NULL,
  body VARCHAR(64) NOT NULL,
  PRIMARY KEY (note_id),
  KEY ix_km_note_item (order_id, line_no, item_no),
  CONSTRAINT fk_km_note_item FOREIGN KEY (order_id, line_no, item_no) REFERENCES km_item (order_id, line_no, item_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO km_line
WITH RECURSIVE o(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM o WHERE n < 100),
               l(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM l WHERE n < 3)
SELECT o.n, l.n, CONCAT('sku-', o.n, '-', l.n) FROM o CROSS JOIN l;

INSERT INTO km_item
SELECT order_id, line_no, i.n, order_id * 10 + line_no
FROM km_line CROSS JOIN (SELECT 1 AS n UNION ALL SELECT 2) i;

INSERT INTO km_item_note
SELECT ROW_NUMBER() OVER (ORDER BY order_id, line_no, item_no), order_id, line_no, item_no,
       CONCAT('note ', order_id, '/', line_no, '/', item_no)
FROM km_item;

-- 200 BINARY(16) keys: the first 16 bytes of SHA-256(n), which are not valid UTF-8
-- for most n. old = 1 on 120 of them (n <= 120).
CREATE TABLE km_blob (
  k BINARY(16) NOT NULL,
  n INT NOT NULL,
  old TINYINT NOT NULL,
  PRIMARY KEY (k)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 2 references per blob, on their own key.
CREATE TABLE km_blob_ref (
  id INT NOT NULL,
  k BINARY(16) NOT NULL,
  PRIMARY KEY (id),
  KEY ix_km_blob_ref_k (k),
  CONSTRAINT fk_km_blob_ref_blob FOREIGN KEY (k) REFERENCES km_blob (k)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO km_blob
WITH RECURSIVE b(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM b WHERE n < 200)
SELECT UNHEX(LEFT(SHA2(n, 256), 32)), n, n <= 120 FROM b;

INSERT INTO km_blob_ref
SELECT ROW_NUMBER() OVER (ORDER BY n, r.i), k
FROM km_blob CROSS JOIN (SELECT 1 AS i UNION ALL SELECT 2) r;

-- 90 latin1 keys with non-ASCII characters. old = 1 on 60 of them.
CREATE TABLE km_name (
  name VARCHAR(40) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  n INT NOT NULL,
  old TINYINT NOT NULL,
  PRIMARY KEY (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 2 tags per name, on their own key.
CREATE TABLE km_name_tag (
  id INT NOT NULL,
  name VARCHAR(40) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  tag VARCHAR(16) NOT NULL,
  PRIMARY KEY (id),
  KEY ix_km_name_tag_name (name),
  CONSTRAINT fk_km_name_tag_name FOREIGN KEY (name) REFERENCES km_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO km_name
WITH RECURSIVE w(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM w WHERE n < 90)
SELECT CONCAT(ELT(1 + n % 3, 'Ñandú', 'Åsa', 'naïve'), '-', LPAD(n, 3, '0')), n, n <= 60 FROM w;

INSERT INTO km_name_tag
SELECT ROW_NUMBER() OVER (ORDER BY n, t.i), name, CONCAT('tag-', t.i)
FROM km_name CROSS JOIN (SELECT 1 AS i UNION ALL SELECT 2) t;
