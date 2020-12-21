    --------------------------------------------------------------------------------
    -- Name: FA_MIGR_DSGR_MP_COV
    -------------------------------------
    -- Purpose: Poller for DESGRAVAMEN (2012) product migration 
    -- Type: TABLE
    -- Versioning:
    --     LPV-FRAMEND0     2020-09-08      1. Created this table
    --     LPV-FRAMEND0     2020-09-15      Added Marks Special Commission fields
    --     LPV-FRAMEND0     2020-10-18      Expand cover_name 
    ---------------------------------------------------------------------------------
   
--------------------------------------------------------
--  DDL for Table FA_MIGR_DSGR_MP_COV
--------------------------------------------------------
DROP TABLE cust_migration.fa_migr_dsgr_mp_cov;

CREATE TABLE cust_migration.fa_migr_dsgr_mp_cov (
    control_id                   NUMBER(30),
    stag_id                      NUMBER(30),
    policy_no                    VARCHAR2(50),
    plan_name                    VARCHAR2(200),
    subplan_name                 VARCHAR2(200),
    manual_prem_dim_desc         VARCHAR2(50),
    prem_rate                    NUMBER,
    prem_value                   NUMBER,
    plan_max_age                 NUMBER,
    plan_min_age                 NUMBER,
    max_outstand                 NUMBER,
    min_outstand                 NUMBER,
    max_iv                       NUMBER,
    max_loan_dur                 VARCHAR2(20),
    min_loan_dur                 VARCHAR2(20),
    main_add_cover_flag          VARCHAR2(20),
    cover_type                   VARCHAR2(10),
    cover_name                   VARCHAR2(100),
    gu_comercial_premium         VARCHAR2(50),
    gu_lp_premium                VARCHAR2(50),
    mark_c_spec_comm_type        VARCHAR2(10),
    mark_c_spec_comm             NUMBER,
    mark_c_spec_dim              VARCHAR2(20),
    mark_gu_coll_spec_comm_type  VARCHAR2(10),
    mark_gu_coll_spec_comm       NUMBER,
    mark_gu_coll_spec_dim        VARCHAR2(20),
    mark_gu_adq_spec_comm_type   VARCHAR2(10),
    mark_gu_adq_spec_comm        NUMBER,
    mark_gu_adq_spec_dim         VARCHAR2(20)
);


--------------------------------------------------------
--  DDL for Index FA_MIGR_DSGR_MP_COV_PK
--------------------------------------------------------

CREATE UNIQUE INDEX cust_migration.fa_migr_dsgr_mp_cov_pk ON
    cust_migration.fa_migr_dsgr_mp_cov (
        control_id,
        stag_id
    );
--------------------------------------------------------
--  Constraints for Table FA_MIGR_DSGR_MP_COV
--------------------------------------------------------

ALTER TABLE cust_migration.fa_migr_dsgr_mp_cov
    ADD CONSTRAINT fa_migr_dsgr_mp_cov_pk PRIMARY KEY ( control_id,
                                                        stag_id )
        USING INDEX enable;
        
--alter table cust_migration.fa_migr_dsgr_mp_cov modify cover_name  VARCHAR2(100);       