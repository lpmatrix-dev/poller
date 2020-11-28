create or replace PACKAGE FA_CUST_MIGR_SCTR AS
    --------------------------------------------------------------------------------
    -- Name: FA_CUST_MIGR_SCTR
    -------------------------------------
    -- Purpose: Poller for SCTR (2010) product migration 
    -- Type: PACKAGE
    -- Versioning:
    --     LPV-FRAMEND0     2020-03-01      creation
    --     LPV-FRAMEND0     2020-03-10      changes in data type
    --     LPV-FRAMEND0     2020-04-01      ISS024-Date datatype changed to varchar
    --     LPV-FRAMEND0     2020-04-02      ISS029-Added Final policy status 
    --     LPV-FRAMEND0     2020-04-14      ISS040-Objects standarization 
    --     LPV-FRAMEND0     2020-09-28      ISS096-Remove mining flag 
    --     LPV-FRAMEND0     2020-09-28      ISS104-Added internalagent & economic group
    ---------------------------------------------------------------------------------


--
--------------------------------------------------------------------------------
-- Name: FA_CUST_MIGR_SCTR.UPLOAD_FA_CUST_MIGR_SCTR
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
        pi_control_id              IN FA_MIGR_SCTR_STG.CONTROL_ID%TYPE,
        pi_stag_id                 IN FA_MIGR_SCTR_STG.STAG_ID%TYPE,
        pi_rowseq                  IN FA_MIGR_SCTR_STG.ROWSEQ%TYPE,
        pi_insis_product_code      IN FA_MIGR_SCTR_STG.INSIS_PRODUCT_CODE%TYPE,
        pi_as_is_product_code      IN FA_MIGR_SCTR_STG.AS_IS_PRODUCT_CODE%TYPE,
        pi_policy_state            IN FA_MIGR_SCTR_STG.POLICY_STATE%TYPE,  
        pi_internal_agent_no       IN FA_MIGR_SCTR_STG.internal_agent_no%TYPE, 
        pi_internal_agent_name     IN FA_MIGR_SCTR_STG.internal_agent_name%TYPE, 
        pi_econo_group_code        IN FA_MIGR_SCTR_STG.econo_group_code%TYPE,
        pi_econo_group_name        IN FA_MIGR_SCTR_STG.econo_group_name%TYPE,
        pi_policy_name             IN FA_MIGR_SCTR_STG.POLICY_NAME%TYPE,
        pi_policy_holder_code      IN FA_MIGR_SCTR_STG.POLICY_HOLDER_CODE%TYPE,
        pi_broker_code             IN FA_MIGR_SCTR_STG.BROKER_CODE%TYPE,
        pi_sales_channel_code      IN FA_MIGR_SCTR_STG.SALES_CHANNEL_CODE%TYPE,
        pi_commiss_perc            IN FA_MIGR_SCTR_STG.COMMISS_PERC%TYPE,
        pi_office_number           IN FA_MIGR_SCTR_STG.OFFICE_NUMBER%TYPE,
        pi_activity_code           IN FA_MIGR_SCTR_STG.ACTIVITY_CODE%TYPE,
        pi_activity_detail         IN FA_MIGR_SCTR_STG.ACTIVITY_DETAIL%TYPE,
        pi_section_code            IN FA_MIGR_SCTR_STG.SECTION_CODE%TYPE,
        pi_currency_code           IN FA_MIGR_SCTR_STG.CURRENCY_CODE%TYPE,
        pi_begin_date              IN FA_MIGR_SCTR_STG.BEGIN_DATE%TYPE,
        pi_end_date                IN FA_MIGR_SCTR_STG.END_DATE%TYPE,
        pi_date_covered            IN FA_MIGR_SCTR_STG.DATE_COVERED%TYPE,
        pi_prem_period_code        IN fa_migr_sctr_stg.prem_period_code%TYPE,
        pi_policy_salud            IN fa_migr_sctr_stg.policy_salud%TYPE,
        pi_min_prem_issue          IN FA_MIGR_SCTR_STG.MIN_PREM_ISSUE%TYPE,
        pi_min_prem_attach         IN FA_MIGR_SCTR_STG.MIN_PREM_ATTACH%TYPE,
        pi_iss_exp_percentage      IN FA_MIGR_SCTR_STG.ISS_EXP_PERCENTAGE%TYPE,
        pi_min_iss_expenses        IN FA_MIGR_SCTR_STG.MIN_ISS_EXPENSES%TYPE,
        pi_calculation_type        IN FA_MIGR_SCTR_STG.CALCULATION_TYPE%TYPE,
        pi_billing_type            IN FA_MIGR_SCTR_STG.BILLING_TYPE%TYPE,
        pi_billing_way             IN FA_MIGR_SCTR_STG.BILLING_WAY%TYPE,
        pi_warranty_clause_flag    IN FA_MIGR_SCTR_STG.WARRANTY_CLAUSE_FLAG%TYPE,
        pi_spec_pen_clause_flag    IN FA_MIGR_SCTR_STG.SPEC_PEN_CLAUSE_FLAG%TYPE,
        pi_spec_pen_clause_detail  IN FA_MIGR_SCTR_STG.SPEC_PEN_CLAUSE_DETAIL%TYPE,
        pi_gratuity_flag           IN FA_MIGR_SCTR_STG.GRATUITY_FLAG%TYPE,
        pi_consortium_flag         IN FA_MIGR_SCTR_STG.CONSORTIUM_FLAG%TYPE,
        pi_elec_pol_flag           IN FA_MIGR_SCTR_STG.ELEC_POL_FLAG%TYPE,
        pi_tender_flag             IN FA_MIGR_SCTR_STG.TENDER_FLAG%TYPE,
        pi_wc_rm1                  IN FA_MIGR_SCTR_STG.WC_RM1%TYPE,
        pi_rn_rm1                  IN FA_MIGR_SCTR_STG.RN_RM1%TYPE,
        pi_wc_rm2                  IN FA_MIGR_SCTR_STG.WC_RM2%TYPE,
        pi_wd_rm2                  IN FA_MIGR_SCTR_STG.WD_RM2%TYPE,
        pi_rn_rm2                  IN FA_MIGR_SCTR_STG.RN_RM2%TYPE,
        pi_wc_rm3                  IN FA_MIGR_SCTR_STG.WC_RM3%TYPE,
        pi_wd_rm3                  IN FA_MIGR_SCTR_STG.WD_RM3%TYPE,
        pi_rn_rm3                  IN FA_MIGR_SCTR_STG.RN_RM3%TYPE,
        pi_wc_rm4                  IN FA_MIGR_SCTR_STG.WC_RM4%TYPE,
        pi_wd_rm4                  IN FA_MIGR_SCTR_STG.WD_RM4%TYPE,
        pi_rn_rm4                  IN FA_MIGR_SCTR_STG.RN_RM4%TYPE,
        pi_wc_rm5                  IN FA_MIGR_SCTR_STG.WC_RM5%TYPE,
        pi_wd_rm5                  IN FA_MIGR_SCTR_STG.WD_RM5%TYPE,
        pi_rn_rm5                  IN FA_MIGR_SCTR_STG.RN_RM5%TYPE
    );

--
--------------------------------------------------------------------------------
-- Name: FA_CUST_MIGR_SCTR.sctr_wrapper
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
    PROCEDURE sctr_wrapper (
        pi_control_id   IN  NUMBER,
        pi_file_id      IN  NUMBER,
        pi_file_name    IN  VARCHAR2,
        pi_poller_name  IN  VARCHAR2
    );

--
--------------------------------------------------------------------------------
-- Name: FA_CUST_MIGR_SCTR.sctr_job_proc
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
    PROCEDURE sctr_job_proc (
        pi_control_id   IN  NUMBER,
        pi_stag_init    IN  NUMBER,
        pi_stag_end     IN  NUMBER,
        pi_file_id      IN  NUMBER,
        pi_file_name    IN  VARCHAR2,
        pi_poller_name  IN  VARCHAR2
    );

--
--------------------------------------------------------------------------------
-- Name: FA_CUST_MIGR_SCTR.sctr_record_proc
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
    PROCEDURE sctr_record_proc (
        pi_fa_sctr_row  IN      cust_migration.fa_migr_sctr_stg%rowtype,
        pio_errmsg      IN OUT  srverr
    );

--
--------------------------------------------------------------------------------
-- Name: FA_CUST_MIGR_SCTR.get_last_record_for_report
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
-- Name: FA_CUST_MIGR_SCTR.upd_last_record_report
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
    PROCEDURE upd_last_record_for_report (
        pi_control_id_rep   IN  NUMBER,
        pi_file_id          IN  NUMBER,
        pi_control_id_proc  IN  NUMBER
    );

--
--------------------------------------------------------------------------------
-- Name: FA_CUST_MIGR_SCTR.ins_error_stg
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
-- Purpose: Record an error 
--
-- Input parameters: 
--
-- Output parameters:
--
--------------------------------------------------------------------------------
    PROCEDURE ins_error_stg (
        pi_control_id  IN      fa_migr_sctr_err.control_id%type,
        pi_stag_id       IN      fa_migr_sctr_err.stag_id%type,
--        pi_errseq       IN      fa_migr_vley_err.errseq%type,
        pi_errtype      IN      fa_migr_sctr_err.errtype%type,
        pi_errcode      in      fa_migr_sctr_err.errcode%type,
        pi_errmess      IN      fa_migr_sctr_err.errmess%type,
        pio_errmsg      IN OUT  srverr

    );

--
--------------------------------------------------------------------------------
-- Name: FA_CUST_MIGR_SCTR.putlog 
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
-- Purpose: Record log
--
-- Input parameters: 
--
-- Output parameters:
--
--------------------------------------------------------------------------------
    PROCEDURE putlog (
        pi_sys_ctrl_id  IN  NUMBER,
        pi_stg_id       IN  NUMBER,
        pi_msg          VARCHAR
    );

END FA_CUST_MIGR_SCTR;