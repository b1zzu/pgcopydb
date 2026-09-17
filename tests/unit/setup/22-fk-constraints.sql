-- Fixture for the opt-in parallel two-phase FOREIGN KEY build (--fk-jobs).
--
-- Covers: a hub table referenced by several children (the shape --fk-jobs
-- targets), a self-referencing FK, a composite-key FK, a DEFERRABLE
-- INITIALLY DEFERRED FK, and an FK that is already NOT VALID on the source
-- (which must stay NOT VALID on the target, never validated).

DROP TABLE IF EXISTS fk_child_b;
DROP TABLE IF EXISTS fk_child_a;
DROP TABLE IF EXISTS fk_nv_child;
DROP TABLE IF EXISTS fk_def_child;
DROP TABLE IF EXISTS fk_comp_child;
DROP TABLE IF EXISTS fk_comp_parent;
DROP TABLE IF EXISTS fk_tree;
DROP TABLE IF EXISTS fk_hub;

CREATE TABLE fk_hub (
    id integer PRIMARY KEY,
    v  text
);

INSERT INTO fk_hub SELECT g, 'v' || g FROM generate_series(1, 100) g;

-- Two children referencing the same hub table: this is the case
-- --fk-jobs is meant to speed up (both VALIDATE CONSTRAINT concurrently).
CREATE TABLE fk_child_a (
    id     integer PRIMARY KEY,
    hub_id integer REFERENCES fk_hub (id)
);

INSERT INTO fk_child_a SELECT g, g FROM generate_series(1, 100) g;

CREATE TABLE fk_child_b (
    id     integer PRIMARY KEY,
    hub_id integer REFERENCES fk_hub (id)
);

INSERT INTO fk_child_b SELECT g, g FROM generate_series(1, 100) g;

-- Self-referencing FK.
CREATE TABLE fk_tree (
    id        integer PRIMARY KEY,
    parent_id integer REFERENCES fk_tree (id)
);

INSERT INTO fk_tree VALUES (1, NULL), (2, 1), (3, 1), (4, 2);

-- Composite-key FK.
CREATE TABLE fk_comp_parent (
    a integer,
    b integer,
    PRIMARY KEY (a, b)
);

INSERT INTO fk_comp_parent VALUES (1, 1), (1, 2), (2, 1);

CREATE TABLE fk_comp_child (
    id integer PRIMARY KEY,
    x  integer,
    y  integer,
    CONSTRAINT fk_comp_child_fkey FOREIGN KEY (x, y) REFERENCES fk_comp_parent (a, b)
);

INSERT INTO fk_comp_child VALUES (1, 1, 1), (2, 1, 2);

-- DEFERRABLE INITIALLY DEFERRED FK.
CREATE TABLE fk_def_child (
    id     integer PRIMARY KEY,
    hub_id integer,
    CONSTRAINT fk_def_child_fkey FOREIGN KEY (hub_id) REFERENCES fk_hub (id)
        DEFERRABLE INITIALLY DEFERRED
);

INSERT INTO fk_def_child VALUES (1, 1), (2, 2);

-- Already NOT VALID on the source: must stay NOT VALID on the target, and
-- must never be validated by Phase B (the underlying row genuinely violates
-- the constraint).
CREATE TABLE fk_nv_child (
    id     integer PRIMARY KEY,
    hub_id integer
);

INSERT INTO fk_nv_child VALUES (1, 999999);

ALTER TABLE fk_nv_child
    ADD CONSTRAINT fk_nv_child_fkey FOREIGN KEY (hub_id) REFERENCES fk_hub (id)
    NOT VALID;
