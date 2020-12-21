create or replace PACKAGE                fa_cust_migr_dsgr_mp AS  
    --
    --------------------------------------------------------------------------------
    -- Name: fa_cust_migr_dsgr_mp
    -------------------------------------
    -- Purpose: Poller for 2012-DESGRAVAMEN Master Policies migration 
    -- Type: TABLE
    -- Versioning:
    --     LPV-FRAMEND0     2020-09-08      1. Created this package
    --     
    ------------------------------  ---------------------------------------------------
    cn_stat_rec_load    CONSTANT VARCHAR2(5) := 'LOAD';
    cn_stat_rec_valid   CONSTANT VARCHAR2(5) := 'VALID';
    cn_stat_rec_error   CONSTANT VARCHAR2(5) := 'ERROR';
    
    cn_report_gen       CONSTANT VARCHAR2(3) := 'GEN';
    cn_ready_for_rep    CONSTANT VARCHAR2(3) := 'REP';
    
    cn_poller_code      CONSTANT VARCHAR2(30) := 'XLS_MIGR_DSGR_MP';
    cn_poller_object    CONSTANT VARCHAR2(30) := 'fa_cust_migr_dsgr_mp';
    
--
--------------------------------------------------------------------------------
-- Name: fa_cust_migr_dsgr_mp.upload_row_pol
--
-- Type: PROCEDURE
--
-- Subtype: DATA_CHECK
--
-- Status: ACTIVE
--
-- Versioning:
--     La Positiva   07.10.2019  creation
--
-- Purpose: Load a row of policy data
--
-- Input parameters:
--    sys_ctrl_id 
--    file_id 
--    file_name 
--
-- Output parameters:
--
--------------------------------------------------------------------------------
        PROCEDURE upload_row_pol (
        pi_control_id                  fa_migr_dsgr_mp_pol.control_id%TYPE,
        pi_stag_id                     fa_migr_dsgr_mp_pol.stag_id%TYPE,
        pi_pholder_pid                 fa_migr_dsgr_mp_pol.pholder_pid%TYPE,
        pi_pholder_name                fa_migr_dsgr_mp_pol.pholder_name%TYPE,
        pi_financial_ent_pid           fa_migr_dsgr_mp_pol.financial_ent_pid%TYPE,
        pi_financial_ent_name          fa_migr_dsgr_mp_pol.financial_ent_name%TYPE,
        pi_payor_pid                   fa_migr_dsgr_mp_pol.payor_pid%TYPE,
        pi_payor_name                  fa_migr_dsgr_mp_pol.payor_name%TYPE,
        pi_asis_code                   fa_migr_dsgr_mp_pol.asis_code%TYPE,
        pi_asis_name                   fa_migr_dsgr_mp_pol.asis_name%TYPE,
        pi_internal_ag_pid             fa_migr_dsgr_mp_pol.internal_ag_pid%TYPE,
        pi_internal_ag_name            fa_migr_dsgr_mp_pol.internal_ag_name%TYPE,
        pi_policy_no                   fa_migr_dsgr_mp_pol.policy_no%TYPE,
        pi_insr_begin                  fa_migr_dsgr_mp_pol.insr_begin%TYPE,
        pi_insr_end                    fa_migr_dsgr_mp_pol.insr_end%TYPE,
        pi_policy_state_desc           fa_migr_dsgr_mp_pol.policy_state_desc%TYPE,
        pi_currency                    fa_migr_dsgr_mp_pol.currency%TYPE,
        pi_sales_channel_id            fa_migr_dsgr_mp_pol.sales_channel_id%TYPE,
        pi_sales_channel_desc          fa_migr_dsgr_mp_pol.sales_channel_desc%TYPE,
        pi_office_lp_no                fa_migr_dsgr_mp_pol.office_lp_no%TYPE,
        pi_office_lp_name              fa_migr_dsgr_mp_pol.office_lp_name%TYPE,
        pi_epolicy_flag                fa_migr_dsgr_mp_pol.epolicy_flag%TYPE,
        pi_pay_frequency_desc          fa_migr_dsgr_mp_pol.pay_frequency_desc%TYPE,
        pi_billing_type_desc           fa_migr_dsgr_mp_pol.billing_type_desc%TYPE,
        pi_billing_party_desc          fa_migr_dsgr_mp_pol.billing_party_desc%TYPE,
        pi_minimum_prem              IN      Varchar2,  --fa_migr_dsgr_mp_pol.minimum_prem%TYPE,
        pi_iv_type_desc                fa_migr_dsgr_mp_pol.iv_type_desc%TYPE,
        pi_iss_expense_perc            fa_migr_dsgr_mp_pol.iss_expense_perc%TYPE,
        pi_unidentified_io_flag        fa_migr_dsgr_mp_pol.unidentified_io_flag%TYPE,
        pi_consortium_flag             fa_migr_dsgr_mp_pol.consortium_flag%TYPE,
        pi_consortium_leader           fa_migr_dsgr_mp_pol.consortium_leader%TYPE,
        pi_term_disease_perc           fa_migr_dsgr_mp_pol.term_disease_perc%TYPE,
        pi_broker_pid                  fa_migr_dsgr_mp_pol.broker_pid%TYPE,
        pi_broker_name                 fa_migr_dsgr_mp_pol.broker_name%TYPE,
        pi_broker_com_perc           IN      Varchar2,  --fa_migr_dsgr_mp_pol.broker_com_perc%TYPE,
        pi_marketer_c_pid              fa_migr_dsgr_mp_pol.marketer_c_pid%TYPE,
        pi_marketer_c_name             fa_migr_dsgr_mp_pol.marketer_c_name%TYPE,
        pi_marketer_comm             IN      Varchar2,  --fa_migr_dsgr_mp_pol.marketer_comm%TYPE,
        pi_marketer_gu_coll_pid        fa_migr_dsgr_mp_pol.marketer_gu_coll_pid%TYPE,
        pi_marketer_gu_coll_name       fa_migr_dsgr_mp_pol.marketer_gu_coll_name%TYPE,
        pi_marketer_gu_coll_comm      IN      Varchar2, --fa_migr_dsgr_mp_pol.marketer_gu_coll_comm%TYPE,
        pi_marketer_gu_acq_pid         fa_migr_dsgr_mp_pol.marketer_gu_acq_pid%TYPE,
        pi_marketer_gu_acq_name        fa_migr_dsgr_mp_pol.marketer_gu_acq_name%TYPE,
        pi_marketer_gu_acq_comm       IN      Varchar2, --fa_migr_dsgr_mp_pol.marketer_gu_acq_comm%TYPE,
        pi_marketer_ps_pid             fa_migr_dsgr_mp_pol.marketer_ps_pid%TYPE,
        pi_marketer_ps_name            fa_migr_dsgr_mp_pol.marketer_ps_name%TYPE,
        pi_marketer_ps_comm            fa_migr_dsgr_mp_pol.marketer_ps_comm%TYPE,
        pi_benef_prov_pid              fa_migr_dsgr_mp_pol.benef_prov_pid%TYPE,
        pi_benef_prov_name             fa_migr_dsgr_mp_pol.benef_prov_name%TYPE,
        pi_expense_deduc_prem_perc     fa_migr_dsgr_mp_pol.expense_deduc_prem_perc%TYPE,
        pi_benef_prov_amount          IN      Varchar2, --fa_migr_dsgr_mp_pol.benef_prov_amount%TYPE,
        pi_assist_type                 fa_migr_dsgr_mp_pol.assist_type%TYPE,
        pi_special_clauses             fa_migr_dsgr_mp_pol.special_clauses%TYPE,
        pi_uw_min_entry_age            fa_migr_dsgr_mp_pol.uw_min_entry_age%TYPE,
        pi_uw_max_entry_age            fa_migr_dsgr_mp_pol.uw_max_entry_age%TYPE,
        pi_auto_indem_max_amount       fa_migr_dsgr_mp_pol.auto_indem_max_amount%TYPE,
        pi_loan_type                   fa_migr_dsgr_mp_pol.loan_type%TYPE,
        pi_main_cov_max_iv             fa_migr_dsgr_mp_pol.main_cov_max_iv%TYPE,
        pi_main_cov_min_iv             fa_migr_dsgr_mp_pol.main_cov_min_iv%TYPE,
        pi_main_io_max_perm_age        fa_migr_dsgr_mp_pol.main_io_max_perm_age%TYPE,
        pi_coinsurance_foll_flag       fa_migr_dsgr_mp_pol.coinsurance_foll_flag%TYPE,
        pi_coinsurance_lead_flag       fa_migr_dsgr_mp_pol.coinsurance_lead_flag%TYPE,
        pi_auto_reinsurance_flag       fa_migr_dsgr_mp_pol.auto_reinsurance_flag%TYPE,
        pi_facul_reinsurance_flag      fa_migr_dsgr_mp_pol.facul_reinsurance_flag%TYPE
    );

--
--------------------------------------------------------------------------------
-- Name: fa_cust_migr_dsgr_mp.upload_row_cov
--
-- Type: PROCEDURE
--
-- Subtype: DATA_CHECK
--
-- Status: ACTIVE
--
-- Versioning:
--     La Positiva   07.10.2019  creation
--
-- Purpose: Load a row of policy data
--
-- Input parameters:
--    sys_ctrl_id 
--    file_id 
--    file_name 
--
-- Output parameters:
--
--------------------------------------------------------------------------------

        PROCEDURE upload_row_cov (
        pi_control_id                  fa_migr_dsgr_mp_cov.control_id%TYPE,
        pi_stag_id                     fa_migr_dsgr_mp_cov.stag_id%TYPE,
        pi_policy_no                   fa_migr_dsgr_mp_cov.policy_no%TYPE,
        pi_plan_name                   fa_migr_dsgr_mp_cov.plan_name%TYPE,
        pi_subplan_name                fa_migr_dsgr_mp_cov.subplan_name%TYPE,
        pi_manual_prem_dim_desc        fa_migr_dsgr_mp_cov.manual_prem_dim_desc%TYPE,
        pi_prem_rate             IN      Varchar2,
        pi_prem_value          IN      Varchar2,        --fa_migr_dsgr_mp_cov.prem_value%TYPE,
        pi_plan_max_age                fa_migr_dsgr_mp_cov.plan_max_age%TYPE,
        pi_plan_min_age                fa_migr_dsgr_mp_cov.plan_min_age%TYPE,
        pi_max_outstand                fa_migr_dsgr_mp_cov.max_outstand%TYPE,
        pi_min_outstand                fa_migr_dsgr_mp_cov.min_outstand%TYPE,
        pi_max_iv                      fa_migr_dsgr_mp_cov.max_iv%TYPE,
        pi_max_loan_dur                fa_migr_dsgr_mp_cov.max_loan_dur%TYPE,
        pi_min_loan_dur                fa_migr_dsgr_mp_cov.min_loan_dur%TYPE,
        pi_main_add_cover_flag         fa_migr_dsgr_mp_cov.main_add_cover_flag%TYPE,
        pi_cover_type                  fa_migr_dsgr_mp_cov.cover_type%TYPE,
        pi_cover_name                  fa_migr_dsgr_mp_cov.cover_name%TYPE,
        pi_gu_comercial_premium        fa_migr_dsgr_mp_cov.gu_comercial_premium%TYPE,
        pi_gu_lp_premium               fa_migr_dsgr_mp_cov.gu_lp_premium%TYPE,
        pi_mark_c_spec_comm_type       fa_migr_dsgr_mp_cov.mark_c_spec_comm_type%TYPE,
        pi_mark_c_spec_comm        IN      Varchar2,    --fa_migr_dsgr_mp_cov.mark_c_spec_comm%TYPE,
        pi_mark_c_spec_dim             fa_migr_dsgr_mp_cov.mark_c_spec_dim%TYPE,
        pi_mark_gu_coll_spec_comm_type fa_migr_dsgr_mp_cov.mark_gu_coll_spec_comm_type%TYPE,
        pi_mark_gu_coll_spec_comm    IN      Varchar2,   --fa_migr_dsgr_mp_cov.mark_gu_coll_spec_comm%TYPE,
        pi_mark_gu_coll_spec_dim       fa_migr_dsgr_mp_cov.mark_gu_coll_spec_dim%TYPE,
        pi_mark_gu_adq_spec_comm_type  fa_migr_dsgr_mp_cov.mark_gu_adq_spec_comm_type%TYPE,
        pi_mark_gu_adq_spec_comm     IN      Varchar2,   --fa_migr_dsgr_mp_cov.mark_gu_adq_spec_comm%TYPE,
        pi_mark_gu_adq_spec_dim        fa_migr_dsgr_mp_cov.mark_gu_adq_spec_dim%TYPE
    );

    
--
--------------------------------------------------------------------------------
-- Name: fa_cust_migr_dsgr_mp.process_main
--
-- Type: PROCEDURE
--
-- Subtype: DATA_CHECK
--
-- Status: ACTIVE
--
-- Versioning:
--     La Positiva   07.10.2019  creation
--
-- Purpose: Main poller process. Complete, validate and process data
--
-- Input parameters:
--    sys_ctrl_id 
--    file_id 
--    file_name 
--
-- Output parameters:
--
--------------------------------------------------------------------------------

        PROCEDURE process_main (
        pi_control_id  IN  NUMBER,
        pi_file_id     IN  NUMBER,
        pi_file_name   IN  VARCHAR
    );
--
--------------------------------------------------------------------------------
-- Name: fa_cust_migr_dsgr_mp.process_job
--
-- Type: PROCEDURE
--
-- Subtype: DATA_CHECK
--
-- Status: ACTIVE
--
-- Versioning:
--     La Positiva   07.10.2019  creation
--
-- Purpose:  Process a block of records
--
-- Input parameters:
--    sys_ctrl_id 
--    file_id 
--    file_name 
--
-- Output parameters:
--
--------------------------------------------------------------------------------

        PROCEDURE process_job (
        pi_control_id  IN  NUMBER,
        pi_file_id     IN  NUMBER,
        pi_file_name   IN  VARCHAR,
        pi_id_init     IN  NUMBER,
        pi_id_end      IN  NUMBER,
        pi_id_page     IN  NUMBER
    );
    
    
    --
    -- reverse_process
    -- Revert process
    --
        PROCEDURE reverse_process (
        pi_control_id  IN  NUMBER,
        pi_file_id     IN  NUMBER,
        pi_file_name   IN  VARCHAR
    );


    --
    -- get_last_report_proc
    -- Get last batch number to generate report
    --
        PROCEDURE get_last_report_proc (
        po_poller_id     OUT  NUMBER,
        po_file_name     OUT  VARCHAR2,
        po_success_flag  OUT  INTEGER
    );
    
    --
    -- upd_last_report_proc
    -- Updates status last batch 
    --
        PROCEDURE upd_last_report_proc (
        pi_control_id_rep   IN  NUMBER,
        pi_file_id          IN  NUMBER,
        pi_control_id_proc  IN  NUMBER
    );

END fa_cust_migr_dsgr_mp;