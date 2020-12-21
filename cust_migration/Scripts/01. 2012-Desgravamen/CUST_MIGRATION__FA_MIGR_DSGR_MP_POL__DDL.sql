--------------------------------------------------------------------------------
-- Name: FA_MIGR_DSGR_MP_POL
-------------------------------------
-- Purpose: Poller for DESGRAVAMEN (2012) product migration 
-- Type: TABLE
-- Versioning:
--     LPV-FRAMEND0     2020-09-08      1. Created this table
--     LPV-FRAMEND0     2020-09-15      Added MARK GU ACQ Info
--
---------------------------------------------------------------------------------
   
--------------------------------------------------------
--  DDL for Table FA_MIGR_DSGR_MP_POL
--------------------------------------------------------
DROP TABLE cust_migration.fa_migr_dsgr_mp_pol;

CREATE TABLE cust_migration.fa_migr_dsgr_mp_pol (
    control_id                     NUMBER(30, 0),
    stag_id                        NUMBER(30, 0),
    pholder_pid                    VARCHAR2(50),
    pholder_name                   VARCHAR2(100),
    financial_ent_pid              VARCHAR2(50),
    financial_ent_name             VARCHAR2(100),
    payor_pid                      VARCHAR2(50),
    payor_name                     VARCHAR2(100),
    asis_code                      VARCHAR2(2),
    asis_name                      VARCHAR2(100),
    internal_ag_pid                VARCHAR2(50),
    internal_ag_name               VARCHAR2(100),
    policy_no                      VARCHAR2(50),
    insr_begin                     VARCHAR2(10),
    insr_end                       VARCHAR2(10),
    policy_state_desc              VARCHAR2(5),
    currency                       VARCHAR2(3),
    sales_channel_id               NUMBER(4),
    sales_channel_desc             VARCHAR2(100),
    office_lp_no                   VARCHAR2(4),
    office_lp_name                 VARCHAR2(25),
    epolicy_flag                   VARCHAR2(2),
    pay_frequency_desc             VARCHAR2(15),
    billing_type_desc              VARCHAR2(20),
    billing_party_desc             VARCHAR2(20),
    minimum_prem                   NUMBER,
    iv_type_desc                   VARCHAR2(20),
    iss_expense_perc               NUMBER,
    unidentified_io_flag           VARCHAR2(20),
    consortium_flag                VARCHAR2(2),
    consortium_leader              VARCHAR2(50),
    term_disease_perc              NUMBER,
    broker_pid                     VARCHAR2(50),
    broker_name                    VARCHAR2(100),
    broker_com_perc                NUMBER,
    marketer_c_pid                 VARCHAR2(50),
    marketer_c_name                VARCHAR2(100),
    marketer_comm                  NUMBER,
    marketer_gu_coll_pid           VARCHAR2(50),
    marketer_gu_coll_name          VARCHAR2(100),
    marketer_gu_coll_comm          NUMBER,
    marketer_gu_acq_pid            VARCHAR2(50),
    marketer_gu_acq_name           VARCHAR2(100),
    marketer_gu_acq_comm           NUMBER,
    marketer_ps_pid                VARCHAR2(50),
    marketer_ps_name               VARCHAR2(100),
    marketer_ps_comm               NUMBER,
    benef_prov_pid                 VARCHAR2(50),
    benef_prov_name                VARCHAR2(100),
    expense_deduc_prem_perc        NUMBER,
    benef_prov_amount              NUMBER,
    assist_type                    VARCHAR2(10),
    special_clauses                VARCHAR2(4000),
    uw_min_entry_age               NUMBER,
    uw_max_entry_age               NUMBER,
    auto_indem_max_amount          NUMBER,
    loan_type                      VARCHAR2(20),
    main_cov_max_iv                NUMBER,
    main_cov_min_iv                NUMBER,
    main_io_max_perm_age           NUMBER,
    coinsurance_foll_flag          VARCHAR2(2),
    coinsurance_lead_flag          VARCHAR2(2),
    auto_reinsurance_flag          VARCHAR2(2),
    facul_reinsurance_flag         VARCHAR2(2),
    att_status_row                 VARCHAR2(5),
    att_policy_id                  NUMBER(30),
    att_pholder_manid              NUMBER(10),
    att_financial_ent_manid        NUMBER(10),
    att_payor_manid                NUMBER(10),
    att_internal_agent_id          NUMBER(10),
    att_broker_agent_id            NUMBER(10),
    att_mark_c_agent_id            NUMBER(10),
    att_mark_gu_coll_agent_id      NUMBER(10),
    att_mark_gu_acq_agent_id       NUMBER(10),
    att_mark_ps_agent_id           NUMBER(10),
    att_benef_prov_manid           NUMBER(10)
);

--------------------------------------------------------
--  DDL for Index FA_MIGR_DSGR_MP_POL_PK
--------------------------------------------------------

CREATE UNIQUE INDEX cust_migration.fa_migr_dsgr_mp_pol_pk ON
    cust_migration.fa_migr_dsgr_mp_pol (
        control_id,
        stag_id
    );
--------------------------------------------------------
--  Constraints for Table FA_MIGR_DSGR_MP_POL
--------------------------------------------------------

ALTER TABLE cust_migration.fa_migr_dsgr_mp_pol
    ADD CONSTRAINT fa_migr_dsgr_mp_pol_pk PRIMARY KEY ( control_id,
                                                        stag_id )
        USING INDEX enable;