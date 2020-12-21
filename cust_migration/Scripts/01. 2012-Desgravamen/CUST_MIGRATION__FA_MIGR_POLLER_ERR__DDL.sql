    --------------------------------------------------------------------------------
    -- Name: FA_MIGR_POLL_ERR
    -------------------------------------
    -- Purpose: Error for migration Pollers 
    -- Type: TABLE
    -- Versioning:
    --     LPV-FRAMEND0     2020-09-08      creation
    --
    ---------------------------------------------------------------------------------
DROP TABLE "CUST_MIGRATION"."FA_MIGR_POLLER_ERR";

CREATE TABLE "CUST_MIGRATION"."FA_MIGR_POLLER_ERR" (
    poller_code  VARCHAR2(30),
    control_id   NUMBER,
    stag_id      NUMBER,
    err_seq      NUMBER,
    err_type     VARCHAR2(3),
    err_code     VARCHAR2(50),
    err_mess     VARCHAR2(4000)
);

CREATE UNIQUE INDEX cust_migration.FA_MIGR_POLLER_ERR_pk ON
    cust_migration.FA_MIGR_POLLER_ERR (
        control_id,
        stag_id,
        err_seq
    );
--------------------------------------------------------
--  Constraints for Table FA_MIGR_POLLER_ERR
--------------------------------------------------------

ALTER TABLE cust_migration.FA_MIGR_POLLER_ERR
    ADD CONSTRAINT FA_MIGR_POLLER_ERR_pk PRIMARY KEY ( control_id,
                                                     stag_id,
                                                     err_seq )
        USING INDEX enable;

COMMENT ON TABLE cust_migration.FA_MIGR_POLLER_ERR IS
    'Errors and report control for migration process pollers';