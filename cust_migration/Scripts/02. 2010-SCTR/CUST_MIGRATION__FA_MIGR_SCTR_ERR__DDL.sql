    --------------------------------------------------------------------------------
    -- Name: FA_MIGR_SCTR_ERR
    -------------------------------------
    -- Purpose: Error for Poller for SCTR (2010) product migration 
    -- Type: TABLE
    -- Versioning:
    --     LPV-FRAMEND0     2020-03-01      creation
    --     LPV-FRAMEND0     2020-04-14      ISS040-Objects standarization 
    ---------------------------------------------------------------------------------
DROP TABLE "CUST_MIGRATION"."FA_MIGR_SCTR_ERR";

CREATE TABLE "CUST_MIGRATION"."FA_MIGR_SCTR_ERR" 
    ("CONTROL_ID" NUMBER, 
    "STAG_ID"   NUMBER, 
    "ERRSEQ"    NUMBER,
    "ERRTYPE"   VARCHAR2(3), 
    "ERRCODE"   VARCHAR2(50), 
    "ERRMESS"   VARCHAR2(4000)
    ) ;

CREATE UNIQUE INDEX CUST_MIGRATION.FA_MIGR_SCTR_ERR_PK ON
    CUST_MIGRATION.FA_MIGR_SCTR_ERR (
        control_id,
        stag_id, 
        errseq
    );
--------------------------------------------------------
--  Constraints for Table FA_MIGR_SCTR_ERR
--------------------------------------------------------

ALTER TABLE CUST_MIGRATION.FA_MIGR_SCTR_ERR
    ADD CONSTRAINT FA_MIGR_SCTR_ERR_PK PRIMARY KEY ( control_id, stag_id, errseq )
        USING INDEX enable;

COMMENT ON TABLE CUST_MIGRATION.FA_MIGR_SCTR_ERR is 'Errors in migration process poller for SCTR product (2010). Report control ';