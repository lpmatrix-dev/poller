create or replace PACKAGE                fa_cust_migr_vlt_mp AS


    --------------------------------------------------------------------------------
    -- Name: fa_cust_migr_vlt_mp
    -------------------------------------
    -- Purpose: Poller for VLEY (2009-1) product migration
    -- Type: PACKAGE
    -- Versioning:
    --     LPV-FRAMEND0     2020-03-09      creation
    --     LPV-FRAMEND0     2020-03-09      ISS037-Add policy_state
    --     LPV-FRAMEND0     2020-03-09      ISS039-Add rutines for report
    --     LPV-FRAMEND0     2020-04-27      ISS048-Fix endorsements. Add 601, remove 607
    --     LPV-FRAMEND0     2020-09-03      ISS099-Rename objects
    --     LPV-FRAMEND0     2020-09-03      ISS086-Add Internal agent 0 group
    --     LPV-JAVCANC0     2020-11-08      Sprint7 - Delete legal_limit_flag
    ---------------------------------------------------------------------------------


--
--------------------------------------------------------------------------------
-- Name: fa_cust_migr_vlt_mp.upload_file_data
--
-- Type: PROCEDURE
--
-- Subtype: FILE ADAPTER
--
-- Status: ACTIVE
--
-- Versioning:
--     La Positiva   2020-03-24  creation
--
--
-- Purpose: Load one record from file to poller staging table
--
-- Input parameters: All staging table fields
--
-- Output parameters:
--
--------------------------------------------------------------------------------

    PROCEDURE upload_file_data (
        pi_control_id               IN  fa_migr_vlt_mp_pol.control_id%TYPE,
        pi_stag_id                  IN  fa_migr_vlt_mp_pol.stag_id%TYPE,
        pi_rowseq                   IN  fa_migr_vlt_mp_pol.rowseq%TYPE,
        pi_insr_type                IN  fa_migr_vlt_mp_pol.insr_type%TYPE,
        pi_as_is                    IN  fa_migr_vlt_mp_pol.as_is%TYPE,
        pi_holder_inx_id            IN  fa_migr_vlt_mp_pol.holder_inx_id%TYPE,
        pi_policy_state             IN  fa_migr_vlt_mp_pol.policy_state%TYPE,
        pi_internal_agent_no        IN  fa_migr_vlt_mp_pol.internal_agent_no%TYPE,
        pi_internal_agent_name      IN  fa_migr_vlt_mp_pol.internal_agent_name%TYPE,
        pi_econo_group_code         IN  fa_migr_vlt_mp_pol.econo_group_code%TYPE,
        pi_econo_group_name         IN  fa_migr_vlt_mp_pol.econo_group_name%TYPE,
        pi_master_policy_no         IN  fa_migr_vlt_mp_pol.master_policy_no%TYPE,
        pi_master_begin_date        IN  fa_migr_vlt_mp_pol.master_begin_date%TYPE,
        pi_master_end_date          IN  fa_migr_vlt_mp_pol.master_end_date%TYPE,
        pi_epolicy_flag             IN  fa_migr_vlt_mp_pol.epolicy_flag%TYPE,
        pi_coverdate                IN  fa_migr_vlt_mp_pol.coverdate%TYPE,
        pi_broker_inx_id            IN  fa_migr_vlt_mp_pol.broker_inx_id%TYPE,
        pi_brok_comm_perc           IN  fa_migr_vlt_mp_pol.brok_comm_perc%TYPE,
        pi_currency                 IN  fa_migr_vlt_mp_pol.currency%TYPE,
        pi_channel                  IN  fa_migr_vlt_mp_pol.channel%TYPE,
        pi_office                   IN  fa_migr_vlt_mp_pol.office%TYPE,
        pi_frequency                IN  fa_migr_vlt_mp_pol.frequency%TYPE,
        pi_consortium_flag          IN  fa_migr_vlt_mp_pol.consortium_flag%TYPE,
        pi_tender_flag              IN  fa_migr_vlt_mp_pol.tender_flag%TYPE,
        pi_billing_type             IN  fa_migr_vlt_mp_pol.billing_type%TYPE,
        pi_prem_cal_period          IN  fa_migr_vlt_mp_pol.prem_cal_period%TYPE,
        pi_billing_by               IN  fa_migr_vlt_mp_pol.billing_by%TYPE,
        pi_issuing_min_prem         IN  fa_migr_vlt_mp_pol.issuing_min_prem%TYPE,
        pi_empl1_rate               IN  fa_migr_vlt_mp_pol.empl1_rate%TYPE,
        pi_empl2_rate               IN  fa_migr_vlt_mp_pol.empl2_rate%TYPE,
        pi_high_risk1_rate          IN  fa_migr_vlt_mp_pol.high_risk1_rate%TYPE,
        pi_high_risk2_rate          IN  fa_migr_vlt_mp_pol.high_risk2_rate%TYPE,
        pi_low_risk1_rate           IN  fa_migr_vlt_mp_pol.low_risk1_rate%TYPE,
        pi_low_risk2_rate           IN  fa_migr_vlt_mp_pol.low_risk2_rate%TYPE,
        pi_natdeath_sal             IN  fa_migr_vlt_mp_pol.natdeath_sal%TYPE,
        pi_accdeath_sal             IN  fa_migr_vlt_mp_pol.accdeath_sal%TYPE,
        pi_itpa_sal                 IN  fa_migr_vlt_mp_pol.itpa_sal%TYPE,
        pi_plan                     IN  fa_migr_vlt_mp_pol.PLAN%TYPE,
        pi_legal_cov_flag           IN  fa_migr_vlt_mp_pol.legal_cov_flag%TYPE,
        pi_fe_num_sal               IN  fa_migr_vlt_mp_pol.fe_num_sal%TYPE,
        pi_fe_max_si                IN  fa_migr_vlt_mp_pol.fe_max_si%TYPE,
        pi_desg_num_sal             IN  fa_migr_vlt_mp_pol.desg_num_sal%TYPE,
        pi_desg_max_si              IN  fa_migr_vlt_mp_pol.desg_max_si%TYPE,
        pi_homeless_num_sal         IN  fa_migr_vlt_mp_pol.homeless_num_sal%TYPE,
        pi_homeless_max_si          IN  fa_migr_vlt_mp_pol.homeless_max_si%TYPE,
        pi_anttermill_num_sal       IN  fa_migr_vlt_mp_pol.anttermill_num_sal%TYPE,
        pi_anttermill_max_si        IN  fa_migr_vlt_mp_pol.anttermill_max_si%TYPE,
        pi_cancer_death_num_sal     IN  fa_migr_vlt_mp_pol.cancer_death_num_sal%TYPE,
        pi_cancer_death_max_si      IN  fa_migr_vlt_mp_pol.cancer_death_max_si%TYPE,
        pi_cancer_num_sal           IN  fa_migr_vlt_mp_pol.cancer_num_sal%TYPE,
        pi_cancer_max_si            IN  fa_migr_vlt_mp_pol.cancer_max_si%TYPE,
        pi_critmyo_num_sal          IN  fa_migr_vlt_mp_pol.critmyo_num_sal%TYPE,
        pi_critmyo_max_si           IN  fa_migr_vlt_mp_pol.critmyo_max_si%TYPE,
        pi_cistroke_num_sal         IN  fa_migr_vlt_mp_pol.cistroke_num_sal%TYPE,
        pi_cistroke_max_si          IN  fa_migr_vlt_mp_pol.cistroke_max_si%TYPE,
        pi_cicrf_num_sal            IN  fa_migr_vlt_mp_pol.cicrf_num_sal%TYPE,
        pi_cicrf_max_si             IN  fa_migr_vlt_mp_pol.cicrf_max_si%TYPE,
        pi_cimultscl_num_sal        IN  fa_migr_vlt_mp_pol.cimultscl_num_sal%TYPE,
        pi_cimultscl_max_si         IN  fa_migr_vlt_mp_pol.cimultscl_max_si%TYPE,
        pi_cicoma_num_sal           IN  fa_migr_vlt_mp_pol.cicoma_num_sal%TYPE,
        pi_cicoma_max_si            IN  fa_migr_vlt_mp_pol.cicoma_max_si%TYPE,
        pi_cibypass_num_sal         IN  fa_migr_vlt_mp_pol.cibypass_num_sal%TYPE,
        pi_cibypass_max_si          IN  fa_migr_vlt_mp_pol.cibypass_max_si%TYPE,
        pi_critill_num_sal          IN  fa_migr_vlt_mp_pol.critill_num_sal%TYPE,
        pi_critill_max_si           IN  fa_migr_vlt_mp_pol.critill_max_si%TYPE,
        pi_blindness_num_sal        IN  fa_migr_vlt_mp_pol.blindness_num_sal%TYPE,
        pi_blindness_max_si         IN  fa_migr_vlt_mp_pol.blindness_max_si%TYPE,
        pi_critburn_num_sal         IN  fa_migr_vlt_mp_pol.critburn_num_sal%TYPE,
        pi_critburn_max_si          IN  fa_migr_vlt_mp_pol.critburn_max_si%TYPE,
        pi_posthum_child_num_sal    IN  fa_migr_vlt_mp_pol.posthum_child_num_sal%TYPE,
        pi_posthum_child_max_si     IN  fa_migr_vlt_mp_pol.posthum_child_max_si%TYPE,
        pi_deafness_num_sal         IN  fa_migr_vlt_mp_pol.deafness_num_sal%TYPE,
        pi_deafness_max_si          IN  fa_migr_vlt_mp_pol.deafness_max_si%TYPE,
        pi_fam_sal_perc             IN  fa_migr_vlt_mp_pol.fam_sal_perc%TYPE,
        pi_fam_num_sal              IN  fa_migr_vlt_mp_pol.fam_num_sal%TYPE,
        pi_fam_max_si               IN  fa_migr_vlt_mp_pol.fam_max_si%TYPE,
        pi_reprem_num_sal           IN  fa_migr_vlt_mp_pol.reprem_num_sal%TYPE,
        pi_reprem_max_si            IN  fa_migr_vlt_mp_pol.reprem_max_si%TYPE,
        pi_inabwork_num_sal         IN  fa_migr_vlt_mp_pol.inabwork_num_sal%TYPE,
        pi_inabwork__max_si         IN  fa_migr_vlt_mp_pol.inabwork__max_si%TYPE,
        pi_transfer_num_sal         IN  fa_migr_vlt_mp_pol.transfer_num_sal%TYPE,
        pi_transfer_max_si          IN  fa_migr_vlt_mp_pol.transfer_max_si%TYPE,
        pi_unid_policy_flag         IN  fa_migr_vlt_mp_pol.unid_policy_flag%TYPE,
--        pi_legal_limit_flag         IN  fa_migr_vlt_mp_pol.legal_limit_flag%TYPE,
        pi_legal_limit_clause_flag  IN  fa_migr_vlt_mp_pol.legal_limit_clause_flag%TYPE,
        pi_no_salary_limit_flag     IN  fa_migr_vlt_mp_pol.no_salary_limit_flag%TYPE,
        pi_indem_pay_clause_flag    IN  fa_migr_vlt_mp_pol.indem_pay_clause_flag%TYPE,
        pi_claim_pay_clause_flag    IN  fa_migr_vlt_mp_pol.claim_pay_clause_flag%TYPE,
        pi_currency_clause_flag     IN  fa_migr_vlt_mp_pol.currency_clause_flag%TYPE,
        pi_waiting_clause_flag      IN  fa_migr_vlt_mp_pol.waiting_clause_flag%TYPE,
        pi_special_clause_text      IN  fa_migr_vlt_mp_pol.special_clause_text%TYPE
    );


--
--------------------------------------------------------------------------------
-- Name: fa_cust_migr_vlt_mp.vley_wrapper
--
-- Type: PROCEDURE
--
-- Subtype: FILE ADAPTER
--
-- Status: ACTIVE
--
-- Versioning:
--     La Positiva   2020-03-24  creation
--
--
-- Purpose: Process poller staging table
--
-- Input parameters:
--
-- Output parameters:
--
--------------------------------------------------------------------------------
    PROCEDURE vley_wrapper (
        pi_sys_ctrl_id  IN  NUMBER,
        pi_file_id      IN  NUMBER,
        pi_file_name    IN  VARCHAR2
    );


--
--------------------------------------------------------------------------------
-- Name: fa_cust_migr_vlt_mp.vley_job_proc
--
-- Type: PROCEDURE
--
-- Subtype: FILE ADAPTER
--
-- Status: ACTIVE
--
-- Versioning:
--     La Positiva   2020-03-24  creation
--
--
-- Purpose: Process a dataset from poller staging table
--
-- Input parameters:
--
-- Output parameters:
--
--------------------------------------------------------------------------------
    PROCEDURE vley_job_proc  (
        pi_sys_ctrl_id  IN  NUMBER,
        pi_stg_init     IN  NUMBER,
        pi_stg_end      IN  NUMBER,
        pi_file_id      IN  NUMBER,
        pi_file_name    IN  VARCHAR2
    );


--
--------------------------------------------------------------------------------
-- Name: fa_cust_migr_vlt_mp.vley_job_proc
--
-- Type: PROCEDURE
--
-- Subtype: FILE ADAPTER
--
-- Status: ACTIVE
--
-- Versioning:
--     La Positiva   2020-03-24  creation
--
--
-- Purpose: Process one record from file to poller staging table
--
-- Input parameters:
--
-- Output parameters:
--
--------------------------------------------------------------------------------
    PROCEDURE vley_record_proc (
        pi_fa_vley_row  IN      cust_migration.fa_migr_vlt_mp_pol%ROWTYPE,
        pio_errmsg   IN OUT  srverr
    );


--
--------------------------------------------------------------------------------
-- Name: fa_cust_migr_vlt_mp.get_last_record_for_report
--
-- Type: PROCEDURE
--
-- Subtype: FILE ADAPTER
--
-- Status: ACTIVE
--
-- Versioning:
--     La Positiva   2020-03-24  creation
--
--
-- Purpose: Get last process id to generate a report
--
-- Input parameters:
--
-- Output parameters:
--
--------------------------------------------------------------------------------
    PROCEDURE get_last_record_for_report (
        po_poller_id     OUT  NUMBER,
        po_file_name     OUT  VARCHAR2,
        po_success_flag  OUT  INTEGER
    );


--
--------------------------------------------------------------------------------
-- Name: fa_cust_migr_vlt_mp.upd_last_record_report
--
-- Type: PROCEDURE
--
-- Subtype: FILE ADAPTER
--
-- Status: ACTIVE
--
-- Versioning:
--     La Positiva   2020-03-24  creation
--
--
-- Purpose: Updates last process record after report was generated
--
-- Input parameters:
--
-- Output parameters:
--
--------------------------------------------------------------------------------
    PROCEDURE upd_last_record_report (
        pi_control_id_rep       IN  NUMBER,
        pi_file_id              IN  NUMBER,
        pi_control_id_proc      IN  NUMBER
    );


--
--------------------------------------------------------------------------------
-- Name: fa_cust_migr_vlt_mp.reverse_proc
--
-- Type: PROCEDURE
--
-- Subtype: FILE ADAPTER
--
-- Status: ACTIVE
--
-- Versioning:
--     La Positiva   2020-03-25  creation
--
--
-- Purpose: Reverse a previuos load
--
-- Input parameters:
--
-- Output parameters:
--
--------------------------------------------------------------------------------

    PROCEDURE reverse_proc (
            pi_sys_ctrl_id  IN  NUMBER,
            pi_file_id      IN  NUMBER,
            pi_file_name    IN  VARCHAR
    );

    PROCEDURE ins_error_stg (
        pi_sys_ctrl_id  IN      fa_migr_vley_err.control_id%TYPE,
        pi_stg_id       IN      fa_migr_vley_err.stag_id%TYPE,
        pi_errseq       IN      fa_migr_vley_err.errseq%TYPE,
        pi_errtype      IN      fa_migr_vley_err.errtype%TYPE,
        pi_errcode      IN      fa_migr_vley_err.errcode%TYPE,
        pi_errmess      IN      fa_migr_vley_err.errmess%TYPE,
        pio_errmsg      IN OUT  srverr
    );

    PROCEDURE putlog (
        pi_sys_ctrl_id  IN  NUMBER,
        pi_stg_id       IN  NUMBER,
        pi_msg          IN  VARCHAR
    );

END fa_cust_migr_vlt_mp;