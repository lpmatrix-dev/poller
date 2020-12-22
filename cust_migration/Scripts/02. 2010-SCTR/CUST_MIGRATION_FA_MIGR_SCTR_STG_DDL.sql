--------------------------------------------------------------------------------
    -- Name: FA_MIGR_SCTR_STG
    -------------------------------------
    -- Purpose: Poller for SCTR (2010) product migration 
    -- Type: TABLE
    -- Versioning:
    --     LPV-FRAMEND0     2020-03-01      creation
    --     LPV-FRAMEND0     2020-03-10      changes in data type
    --     LPV-FRAMEND0     2020-04-01      ISS024-Date datatype changed to varchar
    --     LPV-FRAMEND0     2020-04-02      ISS029-Added Final policy status 
    --     LPV-FRAMEND0     2020-04-02      ISS040-Objects standarization 
    --     LPV-FRAMEND0     2020-09-28      ISS096-Remove mining flag 
    --     LPV-FRAMEND0     2020-09-28      ISS104-Added internal agent & economic group
    --     LPV-JAVCANC0     2020-11-17      SPRINT6-Added policy salud for aditional conditions
    --     LPV-JAVCANC0     2020-12-09      SPRINT7-Remove section_code
    ---------------------------------------------------------------------------------
DROP TABLE cust_migration.FA_MIGR_SCTR_STG;

CREATE TABLE cust_migration.FA_MIGR_SCTR_STG (
    control_id              NUMBER,
    stag_id                 NUMBER,
    rowseq                  NUMBER,
    insis_product_code      NUMBER,
    as_is_product_code      NUMBER,
    policy_state            NUMBER(2),
    internal_agent_no       VARCHAR2(10),
    internal_agent_name     VARCHAR2(400),
    econo_group_code        VARCHAR2(20),
    econo_group_name        VARCHAR2(400),
    policy_name             VARCHAR2(50),
    policy_holder_code      VARCHAR2(14),
    broker_code             VARCHAR2(14),
    sales_channel_code      NUMBER,
    commiss_perc            NUMBER,
    office_number           NUMBER,
    activity_code           NUMBER,
    activity_detail         VARCHAR2(250),
--    section_code            NUMBER,
    currency_code           VARCHAR2(3),
    begin_date              VARCHAR2(10),
    end_date                VARCHAR2(10),
    date_covered            VARCHAR2(10),
    prem_period_code        NUMBER,
    policy_salud            NUMBER,
    min_prem_issue          NUMBER,
    min_prem_attach         NUMBER,
    iss_exp_percentage      NUMBER, --issuing expenses percentage 
    min_iss_expenses        NUMBER,
    calculation_type        NUMBER,
    billing_type            NUMBER,
    billing_way             NUMBER,
    warranty_clause_flag    VARCHAR2(1),
    spec_pen_clause_flag    VARCHAR2(1),
    spec_pen_clause_detail  VARCHAR2(4000),
    gratuity_flag           VARCHAR2(1),
    consortium_flag         VARCHAR2(1),
    elec_pol_flag           VARCHAR2(1),
    tender_flag             VARCHAR2(1),
    wc_rm1                  NUMBER,
    rn_rm1                  NUMBER,
    wc_rm2                  NUMBER,
    wd_rm2                  NUMBER,
    rn_rm2                  NUMBER,
    wc_rm3                  NUMBER,
    wd_rm3                  NUMBER,
    rn_rm3                  NUMBER,
    wc_rm4                  NUMBER,
    wd_rm4                  NUMBER,
    rn_rm4                  NUMBER,
    wc_rm5                  NUMBER,
    wd_rm5                  NUMBER,
    rn_rm5                  NUMBER,
    att_policy_id           NUMBER,
    att_message             VARCHAR2(500),
    att_status              CHAR(1),
    att_int_agent_id        NUMBER(10)
);

CREATE UNIQUE INDEX cust_migration.fa_migr_sctr_stg_pk ON
    cust_migration.fa_migr_sctr_stg (
        control_id,
        stag_id
    );
--------------------------------------------------------
--  Constraints for Table LPV_SCTR_MASTER_STG_v2
--------------------------------------------------------

ALTER TABLE cust_migration.fa_migr_sctr_stg
    ADD CONSTRAINT fa_migr_sctr_stg_pk PRIMARY KEY ( control_id,
                                                     stag_id )
        USING INDEX ENABLE;

COMMENT ON TABLE cust_migration.fa_migr_sctr_stg IS
    'Migration Poller for SCTR (2010)';

COMMENT ON COLUMN cust_migration.fa_migr_sctr_stg.policy_state IS
    'Final policys status. 0: REGISTERED, -2: APPLICATION';

COMMENT ON COLUMN cust_migration.fa_migr_sctr_stg.wc_rm1 IS
    'Worker Category';

COMMENT ON COLUMN cust_migration.fa_migr_sctr_stg.wc_rm2 IS
    'Worker Category';

COMMENT ON COLUMN cust_migration.fa_migr_sctr_stg.wc_rm3 IS
    'Worker Category';

COMMENT ON COLUMN cust_migration.fa_migr_sctr_stg.wc_rm4 IS
    'Worker Category';

COMMENT ON COLUMN cust_migration.fa_migr_sctr_stg.wc_rm5 IS
    'Worker Category';

COMMENT ON COLUMN cust_migration.fa_migr_sctr_stg.rn_rm1 IS
    'Rate';

COMMENT ON COLUMN cust_migration.fa_migr_sctr_stg.rn_rm2 IS
    'Rate';

COMMENT ON COLUMN cust_migration.fa_migr_sctr_stg.rn_rm3 IS
    'Rate';

COMMENT ON COLUMN cust_migration.fa_migr_sctr_stg.rn_rm4 IS
    'Rate';

COMMENT ON COLUMN cust_migration.fa_migr_sctr_stg.rn_rm5 IS
    'Rate';

COMMENT ON COLUMN cust_migration.fa_migr_sctr_stg.wd_rm2 IS
    'Worker Category Detail';

COMMENT ON COLUMN cust_migration.fa_migr_sctr_stg.wd_rm3 IS
    'Worker Category Detail';

COMMENT ON COLUMN cust_migration.fa_migr_sctr_stg.wd_rm4 IS
    'Worker Category Detail';

COMMENT ON COLUMN cust_migration.fa_migr_sctr_stg.wd_rm5 IS
    'Worker Category Detail';

GRANT SELECT ON FA_MIGR_SCTR_STG TO INSIS_GEN_V10_RLS;
    