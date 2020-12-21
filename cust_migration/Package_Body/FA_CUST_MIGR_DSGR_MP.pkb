create or replace PACKAGE BODY                fa_cust_migr_dsgr_mp AS
--
--------------------------------------------------------------------------------
-- Name: fa_cust_migr_dsgr_mp
--
-- Type: PACKAGE
--
-- Subtype: 
--
-- Status: ACTIVE
--
-- Versioning:
--     LPV-framend0         2020-09-08  creation
--     LPV-framend0         2020-10-06  ISS108-Complete poller development
--                                          Load plan/group attributes
--                          2020-10-06      Load Policy special commission rates (objects, obj-cover)                
--                          2020-10-09      Load Policy special commission rates (covers)                
--                          2020-10-12      Load additional covers and update values                
--     LPV-framend0         2020-10-13  ISS109-Fix initial version
--                                          Load Master Premium Period (policy.attr5)
--                                          Fix La Positiva office (policy.attr4)
--                                          Load billing party (policy.payment_type)
--                                          Load billing type (policy_engagement_billing.attr1)
--     LPV-framend0         2020-10-16      Updates As Is description for policy_names      
--     LPV-framend0         2020-10-18      Includes participants
---------------------------------------------------------------------------------
--
-- Purpose: Poller for 2012-DESGRAVAMEN Master Policies migration 
--
-- Input parameters:
--
-- Output parameters:
--
--------------------------------------------------------------------------------

    l_log_seq_ini                cust_migration.sta_log.rec_count%TYPE := 1200000000000;
    l_log_seq                    cust_migration.sta_log.rec_count%TYPE := l_log_seq_ini;
    l_log_proc                   cust_migration.sta_log.batch_id%TYPE;
    l_errseq                     cust_migration.fa_migr_sctr_err.errseq%TYPE := 0;
    
    CN_JOBS_NUMBER               CONSTANT NUMBER := 25;
    CN_FINAL_STATUS_APPLICATION  CONSTANT VARCHAR2(3) := 'APL';
    CN_FINAL_STATUS_REGISTERED   CONSTANT VARCHAR2(3) := 'REG';
    --todo: validar que usuario migration no de problemas de configuracion (por ej falta de monedas)
    CN_PROCESS_USER            CONSTANT VARCHAR2(20) := 'insis_gen_v10'; --'CUST_MIGRATION'
    CN_POLICY_USER             CONSTANT VARCHAR2(20) := 'CUST_MIGRATION'; 

        
    CN_INSR_TYPE               CONSTANT NUMBER(4) := 2012;  --todo: se podria tomar de un objeto poliza

--
--------------------------------------------------------------------------------
-- Name: fa_cust_migr_dsgr_mp.putlog
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
-- Purpose: Log process information 
--
-- Input parameters:
--     p_txt            varchar2   text to log
--
-- Output parameters:
--
--------------------------------------------------------------------------------

    PROCEDURE putlog (
        pi_batch_id  VARCHAR2,
        pi_txt       VARCHAR2
    ) AS
    BEGIN
        sta_utils.log_message(pi_table_name => cn_poller_object, pi_batch_id => pi_batch_id, pi_counter => l_log_seq, pi_message =>
        pi_txt);

--        dbms_output.put_line('[' || systimestamp || ']; ' || l_log_seq || '; fa_cust_migr_spf: ' || p_txt);

        l_log_seq := l_log_seq + 1;
    END putlog;

    --------------------------------------------------------------------------------
    -- Name: putlogcontext
    -------------------------------------
    -- Purpose: record information from context in log
    ---------------------------------------------------------------------------------

    PROCEDURE putlogcontext (
        pi_batch_id  VARCHAR2,
        pi_context   srvcontext
    ) AS
        v_text VARCHAR2(4000);
    BEGIN
        FOR r IN pi_context.first..pi_context.last LOOP
            v_text := v_text || r || ']|' || pi_context(r).attrcode || '|' || pi_context(r).attrtype || '|' || pi_context(r).attrformat ||
            '|' || pi_context(r).attrvalue;
        END LOOP;

        putlog(pi_batch_id, v_text);
    END putlogcontext;

    --
    -- record error for a data row 
    --

    PROCEDURE ins_error_stg (
        pi_control_id  IN      fa_migr_poller_err.control_id%TYPE,
        pi_stag_id     IN      fa_migr_poller_err.stag_id%TYPE,
        pi_errtype     IN      fa_migr_poller_err.err_type%TYPE,
        pi_errcode     IN      fa_migr_poller_err.err_code%TYPE,
        pi_errmess     IN      fa_migr_poller_err.err_mess%TYPE,
        pio_errmsg     IN OUT  srverr
    ) IS
        PRAGMA autonomous_transaction;
        l_errmsg srverrmsg;
    BEGIN
        l_errseq := l_errseq + 1;
        INSERT INTO fa_migr_poller_err (
            poller_code,
            control_id,
            stag_id,
            err_seq,
            err_type,
            err_code,
            err_mess
        ) VALUES (
            fa_cust_migr_dsgr_mp.cn_poller_code,
            pi_control_id,
            pi_stag_id,
            l_errseq,
            pi_errtype,
            pi_errcode,
            pi_errmess
        );

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            putlog(pi_control_id, 'ins_error_stg.err|' || pi_errcode || '|' || SQLERRM);
            srv_error.setsyserrormsg(l_errmsg, 'insert_error_stg', SQLERRM);
            srv_error.seterrormsg(l_errmsg, pio_errmsg);
    END ins_error_stg;



    --------------------------------------------------------------------------------
    -- Name: srv_error_set
    -------------------------------------
    -- Purpose: create error in srverr object
    ---------------------------------------------------------------------------------

    PROCEDURE srv_error_set (
        pi_fn_name     IN      VARCHAR2,
        pi_error_code  IN      VARCHAR2,
        pi_error_msg   IN      VARCHAR2,
        pio_errmsg     IN OUT  srverr
    ) AS
        l_errmsg srverrmsg;
    BEGIN
        insis_sys_v10.srv_error.seterrormsg(l_errmsg, pi_fn_name, nvl(pi_error_code, 'SYSERROR'), pi_fn_name || '::' || pi_error_msg);

        insis_sys_v10.srv_error.seterrormsg(l_errmsg, pio_errmsg);
    EXCEPTION
        WHEN OTHERS THEN
            insis_sys_v10.srv_error.setsyserrormsg(l_errmsg, 'srv_error_set', pi_fn_name || '|' || SQLERRM);
            insis_sys_v10.srv_error.seterrormsg(l_errmsg, pio_errmsg);
    END srv_error_set;

    
    --------------------------------------------------------------------------------
    -- Name: tdate
    -------------------------------------
    -- Purpose: convert date to insis format
    ---------------------------------------------------------------------------------

    FUNCTION tdate (
        pi_strdate VARCHAR2
    ) RETURN DATE AS
        l_date DATE;
    BEGIN
        BEGIN
            l_date := to_date(pi_strdate, 'dd/mm/yyyy');
        EXCEPTION
            WHEN OTHERS THEN
                l_date := NULL;
        END;

        RETURN l_date;
    END tdate;


    --------------------------------------------------------------------------------
    -- Name: upload_row_pol
    --------------------------------------------------------------------------------
    -- Purpose: Load info from file to detail table  
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
    ) IS

        cn_proc  VARCHAR2(100) := 'upload_row_pol:' || pi_control_id;
        v_code   VARCHAR2(4000);
        v_errm   VARCHAR2(4000);
        v_id     NUMBER;
    BEGIN

--        putlog(pi_control_id, cn_proc || '|start|params: ' || '|' ||   || '|' || pi_policy_no);

        BEGIN
            SELECT /*+ INDEX_DESC (stg fa_migr_dsgr_mp_pol_PK) */
                stag_id + 1
            INTO v_id
            FROM
                cust_migration.fa_migr_dsgr_mp_pol stg
            WHERE
                    control_id = pi_control_id
                AND ROWNUM = 1;
         
        EXCEPTION
            WHEN no_data_found THEN
                v_id := 1;
        END;
--        putlog(pi_control_id, cn_proc || '|start|params: ' || '|' || v_id || '|' || pi_policy_no);
        INSERT INTO cust_migration.fa_migr_dsgr_mp_pol (
            control_id,
            stag_id,
            pholder_pid,
            pholder_name,
            financial_ent_pid,
            financial_ent_name,
            payor_pid,
            payor_name,
            asis_code,
            asis_name,
            internal_ag_pid,
            internal_ag_name,
            policy_no,
            insr_begin,
            insr_end,
            policy_state_desc,
            currency,
            sales_channel_id,
            sales_channel_desc,
            office_lp_no,
            office_lp_name,
            epolicy_flag,
            pay_frequency_desc,
            billing_type_desc,
            billing_party_desc,
            minimum_prem,
            iv_type_desc,
            iss_expense_perc,
            unidentified_io_flag,
            consortium_flag,
            consortium_leader,
            term_disease_perc,
            broker_pid,
            broker_name,
            broker_com_perc,
            marketer_c_pid,
            marketer_c_name,
            marketer_comm,
            marketer_gu_coll_pid,
            marketer_gu_coll_name,
            marketer_gu_coll_comm,
            marketer_gu_acq_pid,
            marketer_gu_acq_name,
            marketer_gu_acq_comm,
            marketer_ps_pid,
            marketer_ps_name,
            marketer_ps_comm,
            benef_prov_pid,
            benef_prov_name,
            expense_deduc_prem_perc,
            benef_prov_amount,
            assist_type,
            special_clauses,
            uw_min_entry_age,
            uw_max_entry_age,
            auto_indem_max_amount,
            loan_type,
            main_cov_max_iv,
            main_cov_min_iv,
            main_io_max_perm_age,
            coinsurance_foll_flag,
            coinsurance_lead_flag,
            auto_reinsurance_flag,
            facul_reinsurance_flag,
            att_status_row
        ) VALUES (
            pi_control_id,
            v_id,
            pi_pholder_pid,
            pi_pholder_name,
            pi_financial_ent_pid,
            pi_financial_ent_name,
            pi_payor_pid,
            pi_payor_name,
            pi_asis_code,
            pi_asis_name,
            pi_internal_ag_pid,
            pi_internal_ag_name,
            pi_policy_no,
            pi_insr_begin,
            pi_insr_end,
            pi_policy_state_desc,
            pi_currency,
            pi_sales_channel_id,
            pi_sales_channel_desc,
            pi_office_lp_no,
            pi_office_lp_name,
            pi_epolicy_flag,
            pi_pay_frequency_desc,
            pi_billing_type_desc,
            pi_billing_party_desc,
             to_number(pi_minimum_prem,'9999999999D9999999999', 'NLS_NUMERIC_CHARACTERS = ''.,'''), --pi_minimum_prem,
            pi_iv_type_desc,
            pi_iss_expense_perc,
            pi_unidentified_io_flag,
            pi_consortium_flag,
            pi_consortium_leader,
            pi_term_disease_perc,
            pi_broker_pid,
            pi_broker_name,
            to_number(pi_broker_com_perc,'9999999999D9999999999', 'NLS_NUMERIC_CHARACTERS = ''.,'''), --pi_broker_com_perc,
            pi_marketer_c_pid,
            pi_marketer_c_name,
            to_number(pi_marketer_comm,'9999999999D9999999999', 'NLS_NUMERIC_CHARACTERS = ''.,'''),  --pi_marketer_comm,
            pi_marketer_gu_coll_pid,
            pi_marketer_gu_coll_name,
            to_number(pi_marketer_gu_coll_comm,'9999999999D9999999999', 'NLS_NUMERIC_CHARACTERS = ''.,'''), --pi_marketer_gu_coll_comm,
            pi_marketer_gu_acq_pid,
            pi_marketer_gu_acq_name,
            to_number(pi_marketer_gu_acq_comm,'9999999999D9999999999', 'NLS_NUMERIC_CHARACTERS = ''.,'''), --pi_marketer_gu_acq_comm,
            pi_marketer_ps_pid,
            pi_marketer_ps_name,
            pi_marketer_ps_comm,
            pi_benef_prov_pid,
            pi_benef_prov_name,
            pi_expense_deduc_prem_perc,
            to_number(pi_benef_prov_amount,'9999999999D9999999999', 'NLS_NUMERIC_CHARACTERS = ''.,'''), --pi_benef_prov_amount,
            pi_assist_type,
            pi_special_clauses,
            pi_uw_min_entry_age,
            pi_uw_max_entry_age,
            pi_auto_indem_max_amount,
            pi_loan_type,
            pi_main_cov_max_iv,
            pi_main_cov_min_iv,
            pi_main_io_max_perm_age,
            pi_coinsurance_foll_flag,
            pi_coinsurance_lead_flag,
            pi_auto_reinsurance_flag,
            pi_facul_reinsurance_flag,
            CN_STAT_REC_LOAD
        );
--        putlog(pi_control_id, cn_proc || '|end');

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            DECLARE
                v_code    NUMBER;
                v_errm    VARCHAR2(4000);
                l_errmsg  srverr;
            BEGIN
                v_code  := SQLCODE;
                v_errm  := SQLERRM; -- substr(sqlerrm, 1, 150);
                putlog(pi_control_id, cn_proc || '|end_error|' || v_id || '|' || v_errm);
--                insis_cust_lpv.sys_schema_utils.update_poller_process_status(pi_control_id, 'ERROR');

                ins_error_stg(pi_control_id, v_id, 'ERR', 0, v_errm, l_errmsg);
                ROLLBACK;
            END;
    END upload_row_pol;


    --------------------------------------------------------------------------------
    -- Name: upload_row_cov
    --------------------------------------------------------------------------------
    -- Purpose: Load info from file to detail table  
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
    ) IS

        cn_proc  VARCHAR2(100) := 'upload_row_cov:' || pi_control_id;
        v_code   VARCHAR(4000);
        v_errm   VARCHAR(4000);
        v_id     NUMBER;
    BEGIN
--        putlog(pi_control_id, cn_proc || '|start|params: ' || pi_policy_no || '|'|| pi_prem_rate || '|' || to_number(pi_prem_rate,'9999999999D9999999999', 'NLS_NUMERIC_CHARACTERS = ''.,'''));

        BEGIN
            SELECT /*+ INDEX_DESC(stg fa_migr_dsgr_mp_cov_pk) */
                stag_id + 1
            INTO v_id
            FROM
                cust_migration.fa_migr_dsgr_mp_cov stg
            WHERE
                    control_id = pi_control_id
                AND ROWNUM = 1;

        EXCEPTION
            WHEN no_data_found THEN
                v_id := 1;
        END;

        INSERT INTO cust_migration.fa_migr_dsgr_mp_cov (
            control_id,
            stag_id,
            policy_no,
            plan_name,
            subplan_name,
            manual_prem_dim_desc,
            prem_rate,
            prem_value,
            plan_max_age,
            plan_min_age,
            max_outstand,
            min_outstand,
            max_iv,
            max_loan_dur,
            min_loan_dur,
            main_add_cover_flag,
            cover_type,
            cover_name,
            gu_comercial_premium,
            gu_lp_premium,
            mark_c_spec_comm_type,
            mark_c_spec_comm,
            mark_c_spec_dim,
            mark_gu_coll_spec_comm_type,
            mark_gu_coll_spec_comm,
            mark_gu_coll_spec_dim,
            mark_gu_adq_spec_comm_type,
            mark_gu_adq_spec_comm,
            mark_gu_adq_spec_dim
        ) VALUES (
            pi_control_id,
            v_id,
            pi_policy_no,
            pi_plan_name,
            pi_subplan_name,
            pi_manual_prem_dim_desc,
            to_number(pi_prem_rate,'9999999999D9999999999', 'NLS_NUMERIC_CHARACTERS = ''.,'''), --pi_prem_rate
            to_number(pi_prem_value,'9999999999D9999999999', 'NLS_NUMERIC_CHARACTERS = ''.,'''), --pi_prem_value,
            pi_plan_max_age,
            pi_plan_min_age,
            pi_max_outstand,
            pi_min_outstand,
            pi_max_iv,
            pi_max_loan_dur,
            pi_min_loan_dur,
            pi_main_add_cover_flag,
            pi_cover_type,
            pi_cover_name,
            pi_gu_comercial_premium,
            pi_gu_lp_premium,
            pi_mark_c_spec_comm_type,
            to_number(pi_mark_c_spec_comm,'9999999999D9999999999', 'NLS_NUMERIC_CHARACTERS = ''.,'''), --pi_mark_c_spec_comm,
            pi_mark_c_spec_dim,
            pi_mark_gu_coll_spec_comm_type,
            to_number(pi_mark_gu_coll_spec_comm,'9999999999D9999999999', 'NLS_NUMERIC_CHARACTERS = ''.,'''), --pi_mark_gu_coll_spec_comm,
            pi_mark_gu_coll_spec_dim,
            pi_mark_gu_adq_spec_comm_type,
            to_number(pi_mark_gu_adq_spec_comm,'9999999999D9999999999', 'NLS_NUMERIC_CHARACTERS = ''.,'''), --pi_mark_gu_adq_spec_comm,
            pi_mark_gu_adq_spec_dim
        );
--        putlog(pi_control_id, cn_proc || '|end');

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            DECLARE
                v_code    NUMBER;
                v_errm    VARCHAR2(4000);
                l_errmsg  srverr;
            BEGIN
                v_code  := SQLCODE;
                v_errm  := SQLERRM;
                putlog(pi_control_id, cn_proc || '|end_error|' || v_id || '|' || v_errm);
                --insis_cust_lpv.sys_schema_utils.update_poller_process_status(pi_control_id, 'ERROR');

                ins_error_stg(pi_control_id, v_id, 'ERR', 0, v_errm, l_errmsg);
                ROLLBACK;
            END;
    END upload_row_cov;


    

    --------------------------------------------------------------------------------
    -- Name: get_param_value
    --------------------------------------------------------------------------------
    -- Purpose: Return parameter value for a product
    --------------------------------------------------------------------------------

    FUNCTION get_param_value (
        pi_product_code  insis_gen_cfg_v10.cfg_nl_product.product_code%TYPE,
        pi_param_name    insis_gen_cfg_v10.cpr_params.param_name%TYPE
    ) RETURN insis_gen_cfg_v10.cprs_param_value.param_value%TYPE AS
        pi_param_value insis_gen_cfg_v10.cprs_param_value.param_value%TYPE;
    BEGIN
        BEGIN
            SELECT
                pv.param_value
            INTO pi_param_value
            FROM
                     insis_gen_cfg_v10.cfg_nl_product pr
                INNER JOIN insis_gen_cfg_v10.cfg_nl_product_params    pp ON pp.product_link_id = pr.product_link_id
                INNER JOIN insis_gen_cfg_v10.cprs_param_value         pv ON pv.param_value_cpr_id = pp.param_cpr_id
                INNER JOIN insis_gen_cfg_v10.cpr_params               P ON pv.param_id = P.param_cpr_id
            WHERE
                    pr.product_code = pi_product_code
                AND pr.status <> 'C'
                AND P.param_name = pi_param_name;

        EXCEPTION
            WHEN OTHERS THEN
                pi_param_value := 0;
        END;

        RETURN pi_param_value;
    END get_param_value;

    --
    -- get_policy_conditions_value
    --

    FUNCTION get_policy_conditions_value (
        pi_policy_id   insis_gen_v10.policy_conditions.policy_id%TYPE,
        pi_annex_id    insis_gen_v10.policy_conditions.annex_id%TYPE,
        pi_param_name  insis_gen_v10.policy_conditions.cond_type%TYPE
    ) RETURN insis_gen_v10.policy_conditions.cond_value%TYPE AS
        pi_value insis_gen_v10.policy_conditions.cond_value%TYPE;
    BEGIN
        BEGIN
            SELECT
                nvl((
                    SELECT
                        cond_value
                    FROM
                        insis_gen_v10.policy_conditions
                    WHERE
                            policy_id = pi_policy_id
                        AND annex_id = pi_annex_id
                        AND cond_type = pi_param_name
                ), 0)
            INTO pi_value
            FROM
                dual;

        EXCEPTION
            WHEN OTHERS THEN
                pi_value := 0;
        END;

        RETURN pi_value;
    END get_policy_conditions_value;

    --
    -- get_policy_conditions_dimension
    --

    FUNCTION get_policy_conditions_dimension (
        pi_policy_id   insis_gen_v10.policy_conditions.policy_id%TYPE,
        pi_annex_id    insis_gen_v10.policy_conditions.annex_id%TYPE,
        pi_param_name  insis_gen_v10.policy_conditions.cond_type%TYPE
    ) RETURN insis_gen_v10.policy_conditions.cond_dimension%TYPE AS
        pi_value insis_gen_v10.policy_conditions.cond_dimension%TYPE;
    BEGIN
        BEGIN
            SELECT
                nvl((
                    SELECT
                        cond_dimension
                    FROM
                        insis_gen_v10.policy_conditions
                    WHERE
                            policy_id = pi_policy_id
                        AND annex_id = pi_annex_id
                        AND cond_type = pi_param_name
                ), 0)
            INTO pi_value
            FROM
                dual;

        EXCEPTION
            WHEN OTHERS THEN
                pi_value := 0;
        END;

        RETURN pi_value;
    END get_policy_conditions_dimension;



--------------------------------------------------------------------------------
-- Name: complete_spf_data
--------------------------------------------------------------------------------
-- Purpose: Gather necessary data for validation and processing
--------------------------------------------------------------------------------

    FUNCTION complete_data (
        pi_control_id  IN  NUMBER,
        pi_file_id     IN  NUMBER,
        pi_file_name   IN  VARCHAR
    ) RETURN BOOLEAN IS
--

        cn_proc                 VARCHAR2(100) := 'complete_data:' || pi_control_id;
        l_srverrmsg             insis_sys_v10.srverrmsg;
        v_pio_err               srverr;
    --
        v_fa_migr_dsgr_mp_pol    cust_migration.fa_migr_dsgr_mp_pol%ROWTYPE;
        v_flag_error            BOOLEAN;
        pio_err                 srverr;
        v_errm                  VARCHAR(4000);
        v_file_id               NUMBER;
    --
        l_ctxt_in               srvcontext;
        l_err                   srverr;
        l_ctxt_out              srvcontext;
        l_result                BOOLEAN;
    --
        l_mp_policyrecord       insis_gen_v10.srv_policy_data.gpolicyrecord%TYPE; --insis_gen_v10.pi_POLICY_TYPE;
        l_as_is                 insis_gen_v10.policy_conditions.cond_dimension%TYPE;
        l_engagement_id         insis_gen_v10.policy_eng_policies.engagement_id%TYPE;
        v_gstage                VARCHAR(120) := 'complete_data';
        v_step                  VARCHAR(120);
        v_ret                   BOOLEAN := FALSE;
        v_file_workers_count    NUMBER;
        v_file_avg_age_insured  NUMBER;
        v_policy_no_renov       insis_gen_v10.POLICY.policy_no%TYPE;
        l_agent_id              insis_people_v10.p_agents.agent_id%TYPE;
    BEGIN
        l_log_proc    := pi_control_id;
        putlog(pi_control_id, cn_proc || '|start|params: ' || pi_control_id || ',' || pi_file_id || ',' || pi_file_name);

        v_flag_error  := FALSE;
        v_file_id     := pi_file_id;

        FOR r_agent IN (
                --SALES
                SELECT UNIQUE 'INTERNAL' agent_type, internal_ag_pid agent_pid
                FROM cust_migration.fa_migr_dsgr_mp_pol 
                WHERE control_id = complete_data.pi_control_id
                AND internal_ag_pid IS NOT NULL

                UNION

                --BROKER
                SELECT UNIQUE 'BROKER', stg.broker_pid
                FROM cust_migration.fa_migr_dsgr_mp_pol stg
                WHERE control_id = complete_data.pi_control_id
                AND stg.broker_pid IS NOT NULL

                UNION

                --MARKETER C
                SELECT UNIQUE 'MARK_C', stg.marketer_c_pid
                FROM cust_migration.fa_migr_dsgr_mp_pol stg
                WHERE control_id = complete_data.pi_control_id
                AND stg.marketer_c_pid IS NOT NULL

                UNION

                --MARKETER GU COLL and MARKETER GU ACQ
                SELECT UNIQUE 'MARK_GU_COLL', stg.marketer_gu_coll_pid
                FROM cust_migration.fa_migr_dsgr_mp_pol stg
                WHERE control_id = complete_data.pi_control_id
                AND stg.marketer_gu_coll_pid IS NOT NULL

--                UNION
--
--                --MARKETER GU ACQ
--                --Comentar
--                SELECT UNIQUE 'MARK_GU_ACQ', stg.marketer_gu_acq_pid
--                FROM cust_migration.fa_migr_dsgr_mp_pol stg
--                WHERE control_id = complete_data.pi_control_id
--                AND stg.marketer_gu_acq_pid IS NOT NULL

                UNION

                --MARKETER GU ACQ
                SELECT UNIQUE 'MARK_PS', stg.marketer_ps_pid
                FROM cust_migration.fa_migr_dsgr_mp_pol stg
                WHERE control_id = complete_data.pi_control_id
                AND stg.marketer_ps_pid IS NOT NULL
        )
        LOOP
            --todo:usar objetos
--            pp_agent_type
            BEGIN
                SELECT
                    agent_id
                INTO l_agent_id
                FROM
                    insis_people_v10.p_agents pa
                    INNER JOIN insis_people_v10.p_people P ON (P.man_id = pa.man_id)
                WHERE
                    P.egn = r_agent.agent_pid;
    
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    l_agent_id := NULL;
                WHEN OTHERS THEN
                    putlog(pi_control_id, 'select agent_id.err:'||r_agent.agent_pid || ':' ||SQLERRM); 
                    
--                    srv_error_set('select agent_id', 'InsrDurValidate_Agent', sqlerrm, pio_errmsg);
            END;
            
            IF l_agent_id IS NOT NULL THEN
                IF r_agent.agent_type = 'INTERNAL' THEN
                    UPDATE cust_migration.fa_migr_dsgr_mp_pol d
                    SET
                        d.att_internal_agent_id = l_agent_id
                    WHERE
                            d.control_id = complete_data.pi_control_id
                        AND d.internal_ag_pid = r_agent.agent_pid;
                
                ELSIF r_agent.agent_type = 'BROKER' THEN
                    UPDATE cust_migration.fa_migr_dsgr_mp_pol d
                    SET
                        d.att_broker_agent_id = l_agent_id
                    WHERE
                            d.control_id = complete_data.pi_control_id
                        AND d.broker_pid = r_agent.agent_pid;
                
                ELSIF r_agent.agent_type = 'MARK_C' THEN
                    UPDATE cust_migration.fa_migr_dsgr_mp_pol d
                    SET
                        d.att_mark_c_agent_id = l_agent_id
                    WHERE
                            d.control_id = complete_data.pi_control_id
                        AND d.marketer_c_pid = r_agent.agent_pid;
                
                ELSIF r_agent.agent_type = 'MARK_GU_COLL' THEN
                    UPDATE cust_migration.fa_migr_dsgr_mp_pol d
                    SET
                        d.att_mark_gu_coll_agent_id = l_agent_id,
                        d.att_mark_gu_acq_agent_id = l_agent_id
                    WHERE
                            d.control_id = complete_data.pi_control_id
                        AND d.marketer_gu_coll_pid = r_agent.agent_pid;
                
--                ELSIF r_agent.agent_type = 'MARK_GU_ACQ' THEN -- comentar
--                    UPDATE cust_migration.fa_migr_dsgr_mp_pol d
--                    SET
--                        d.att_mark_gu_acq_agent_id = l_agent_id
--                    WHERE
--                            d.control_id = complete_data.pi_control_id
--                        AND d.marketer_gu_acq_pid = r_agent.agent_pid;
                
                ELSIF r_agent.agent_type = 'MARK_PS' THEN
                    UPDATE cust_migration.fa_migr_dsgr_mp_pol d
                    SET
                        d.att_mark_ps_agent_id = l_agent_id
                    WHERE
                            d.control_id = complete_data.pi_control_id
                        AND d.marketer_ps_pid = r_agent.agent_pid;
                END IF;

                
            END IF;
            
            IF SQL%ROWCOUNT = 0 THEN
                putlog(pi_control_id, cn_proc || '***NO SE ACTUALIZO AGENTE_ID: agent_pid, type:' || r_agent.agent_pid || ',' || r_agent.agent_type); 
            END IF;
            
        END LOOP;

        COMMIT;


        FOR r_participant IN (
                SELECT UNIQUE 'PHOLDER' part_role, stg.control_id, stg.pholder_pid part_pid, 
                              stg.att_pholder_manid att_part_manid --this field is null at this point
                FROM cust_migration.fa_migr_dsgr_mp_pol stg
                WHERE stg.control_id = complete_data.pi_control_id
                AND stg.pholder_pid IS NOT NULL
                UNION
                SELECT UNIQUE 'PAYOR', stg.control_id, stg.payor_pid, 
                              stg.att_payor_manid --this field is null at this point
                FROM cust_migration.fa_migr_dsgr_mp_pol stg
                WHERE stg.control_id = complete_data.pi_control_id
                AND stg.payor_pid IS NOT NULL
                UNION
                SELECT UNIQUE 'FIN', stg.control_id, stg.financial_ent_pid, 
                              stg.att_financial_ent_manid --this field is null at this point
                FROM cust_migration.fa_migr_dsgr_mp_pol stg
                WHERE stg.control_id = complete_data.pi_control_id
                AND stg.financial_ent_pid IS NOT NULL
                UNION
                SELECT UNIQUE 'BENEF', stg.control_id, stg.benef_prov_pid, 
                              stg.att_benef_prov_manid --this field is null at this point
                FROM cust_migration.fa_migr_dsgr_mp_pol stg
                WHERE stg.control_id = complete_data.pi_control_id
                AND stg.benef_prov_pid IS NOT NULL
--            
        )
        LOOP
            BEGIN
                SELECT
                    man_id
                INTO 
                    r_participant.att_part_manid
                FROM
                    insis_people_v10.p_people pp
                WHERE
                    pp.egn = r_participant.part_pid;
    
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    r_participant.att_part_manid := NULL;
                WHEN OTHERS THEN
                    putlog(pi_control_id, 'select provider_id.err:'||r_participant.part_pid|| ':' ||SQLERRM); 
--                    srv_error_set('select agent_id', 'InsrDurValidate_Agent', sqlerrm, pio_errmsg);
            END;
            
            IF r_participant.att_part_manid IS NOT NULL THEN
                IF r_participant.part_role = 'PHOLDER' THEN
                    UPDATE cust_migration.fa_migr_dsgr_mp_pol d
                    SET
                        d.att_pholder_manid = r_participant.att_part_manid
                    WHERE
                            d.control_id = r_participant.control_id
                        AND d.pholder_pid = r_participant.part_pid;
                
                ELSIF r_participant.part_role = 'PAYOR' THEN
                    UPDATE cust_migration.fa_migr_dsgr_mp_pol d
                    SET
                        d.att_payor_manid = r_participant.att_part_manid
                    WHERE
                            d.control_id = r_participant.control_id
                        AND d.payor_pid = r_participant.part_pid;
                
                ELSIF r_participant.part_role = 'FIN' THEN
                    UPDATE cust_migration.fa_migr_dsgr_mp_pol d
                    SET
                        d.att_financial_ent_manid = r_participant.att_part_manid
                    WHERE
                            d.control_id = r_participant.control_id
                        AND d.financial_ent_pid = r_participant.part_pid;
                
                ELSIF r_participant.part_role = 'BENEF' THEN
                    UPDATE cust_migration.fa_migr_dsgr_mp_pol d
                    SET
                        d.att_benef_prov_manid = r_participant.att_part_manid
                    WHERE
                            d.control_id = r_participant.control_id
                        AND d.benef_prov_pid = r_participant.part_pid;  
                END IF;
                        
            END IF;  
            
            IF SQL%ROWCOUNT = 0 THEN
                putlog(pi_control_id, cn_proc || '***NO SE ACTUALIZO PART_MANID:' || r_participant.part_pid); 
            END IF;
--            putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'agent_id, type:' || l_agent_id || ',' || l_agent_type); 
        
        END LOOP;

--    --
--    --dependent_policy_info
--    --
--        v_step        := '[dependent_policy_info]';
--        FOR r_dependent_pol IN (
--            SELECT
--                *
--            FROM
--                cust_migration.fa_migr_dsgr_mp_pol
--            WHERE
--                    control_id = complete_data.pi_control_id
--                AND att_master_policy_id IS NOT NULL
--        ) LOOP
--            putlog(cn_proc || '|dependent_policy_info|policy_name: ' || r_dependent_pol.policy_name);
--            
--            l_err                                   := NULL;
--            v_fa_migr_dsgr_mp_pol                      := NULL;
--
----     get dependent policy data
--            r_dependent_pol.att_hhhhhh := get_man_id_by_inx_id(r_dependent_pol.holder_inx_id);
----          ...            
--
----        -- group/subgroup depend on file's worker category
----        get_group_subgroup(r_dependent_pol.att_mpi_policy_id, 
----                                       r_dependent_pol.att_mpi_annex_id,
----                                       r_dependent_pol.att_mpi_insr_type, 
----                                       r_dependent_pol.att_mpi_as_is, 
----                                       r_dependent_pol.worker_category, 
----                                       v_fa_migr_dsgr_mp_pol.att_mpi_groupi_id, 
----                                       v_fa_migr_dsgr_mp_pol.att_mpi_subgroupi_id);
----
----        --if not (r_dependent_pol.att_operation_code = CN_OPER_EMI or r_dependent_pol.att_operation_code = CN_OPER_INC) then
----         putlog (cn_proc||' get_actual_data_policy_by_man_id' );
----         get_actual_data_policy_by_man_id(  r_dependent_pol.att_mpi_policy_id, 
----                                                              r_dependent_pol.policy_no,
----                                                              v_fa_migr_dsgr_mp_pol.att_man_id,
----                                                              v_fa_migr_dsgr_mp_pol.att_mdpi_actual_policy_id, 
----                                                              v_fa_migr_dsgr_mp_pol.att_mdpi_actual_annex_id,
----                                                              v_fa_migr_dsgr_mp_pol.att_mdpi_actual_worker_cat,
----                                                              v_fa_migr_dsgr_mp_pol.att_mdpi_actual_salary, 
----                                                              v_fa_migr_dsgr_mp_pol.att_mdpi_actual_adm_office );
--
--        --complete dependent related data
----            UPDATE cust_migration.fa_migr_dsgr_mp_pol d
----            SET policy_name = replace(policy_name, ' ', ''),
----                att_holder_man_id = r_dependent_pol.att_holder_man_id,
----              ...
----            att_mdpi_actual_policy_id = v_fa_migr_dsgr_mp_pol.att_mdpi_actual_policy_id,
----            att_mdpi_actual_annex_id = v_fa_migr_dsgr_mp_pol.att_mdpi_actual_annex_id,
----            att_mpi_groupi_id =  v_fa_migr_dsgr_mp_pol.att_mpi_groupi_id,
----            att_mpi_subgroupi_id =  v_fa_migr_dsgr_mp_pol.att_mpi_subgroupi_id,
----            WHERE
----                    control_id = r_dependent_pol.control_id
----                AND stag_id = r_dependent_pol.stag_id;
--
--        END LOOP;
--
--        COMMIT;

        putlog(pi_control_id, cn_proc || '|end');
        v_ret         := TRUE;
        RETURN v_ret;
    EXCEPTION
        WHEN OTHERS THEN
            srv_error.setsyserrormsg(l_srverrmsg, 'fa_cust_migr_dsgr_mp.complete', SQLERRM, SQLCODE);
            srv_error.seterrormsg(l_srverrmsg, v_pio_err);
            putlog(pi_control_id, cn_proc || '|end_error|' || SQLERRM);
            v_ret := FALSE;
            RETURN v_ret;
    END complete_data;



--
--------------------------------------------------------------------------------
-- Name: validate_data
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
-- Purpose: Validate data to be processed.
--          Validation runs by columns
--
-- Input parameters:
--    pi_control_id 
--    file_id 
--    file_name 
--
-- Output parameters:
--
--------------------------------------------------------------------------------

    FUNCTION validate_data (
        pi_control_id  IN  NUMBER,
        pi_file_id     IN  NUMBER,
        pi_file_name   IN  VARCHAR
    ) RETURN BOOLEAN IS
--

        cn_proc         VARCHAR2(100) := 'validate_data:' || pi_control_id;
        v_flag_error    BOOLEAN;
        v_errm          VARCHAR(4000);
        v_file_id       NUMBER;
        l_srverrmsg     insis_sys_v10.srverrmsg;
        v_pio_err       srverr;
        v_gstage        VARCHAR(120) := 'validate_data';
    --
        v_cant          NUMBER;
        v_isfatalerror  BOOLEAN;
        v_validation    VARCHAR2(120);
--        v_fields_val    cust_migration.fa_migr_poller_err.err_desc%TYPE;
        v_ret           BOOLEAN := FALSE;
        v_object_type   insis_gen_v10.insured_object.object_type%TYPE;

    --
    --updates all detail records with error 
    -- to be used when one error block all dataset

        PROCEDURE update_stg_err_all (
            pi_control_id IN NUMBER
        ) AS
        BEGIN
            UPDATE cust_migration.fa_migr_dsgr_mp_pol
            SET
                att_status_row = cn_stat_rec_error
            WHERE
                control_id = validate_data.pi_control_id;

            COMMIT;
        END update_stg_err_all;

    --
    --updates detail records with error for all dependent policies related to a master policy 
    --

        PROCEDURE update_stg_err_mp (
            pi_control_id  IN  NUMBER,
            pi_err_code    IN  VARCHAR2
        ) AS
        BEGIN
            FOR r_ins_obj_dtl IN (
                SELECT UNIQUE
                    d.control_id,
                    d.policy_no
                FROM
                         cust_migration.fa_migr_poller_err E
                    INNER JOIN cust_migration.fa_migr_dsgr_mp_pol d ON ( E.control_id = d.control_id
                                                                        AND E.stag_id = d.stag_id )
                WHERE
                        E.control_id = update_stg_err_mp.pi_control_id
                    AND E.err_code = update_stg_err_mp.pi_err_code
            ) LOOP
            --updates with error all records related to a master policy  
                UPDATE cust_migration.fa_migr_dsgr_mp_pol
                SET
                    att_status_row = cn_stat_rec_error
                WHERE
                        control_id = r_ins_obj_dtl.control_id
                    AND policy_no = r_ins_obj_dtl.policy_no;

            END LOOP;

            COMMIT;
        END update_stg_err_mp;

    --
    --updates detail record with error according err_code
    --

        PROCEDURE update_stg_err (
            pi_control_id  IN  NUMBER,
            pi_err_code    IN  VARCHAR2
        ) AS
        BEGIN
            UPDATE cust_migration.fa_migr_dsgr_mp_pol
            SET
                att_status_row = cn_stat_rec_error
            WHERE
                    control_id = update_stg_err.pi_control_id
--     and (att_status_row <> CN_STAT_REC_ERROR or 
                AND stag_id IN (
                    SELECT
                        stag_id
                    FROM
                        cust_migration.fa_migr_poller_err E
                    WHERE
                            E.control_id = update_stg_err.pi_control_id
                        AND E.err_code = update_stg_err.pi_err_code
                );

            putlog(pi_control_id, 'update_stg_err|' || pi_err_code || ':' || SQL%ROWCOUNT);
            COMMIT;
        END update_stg_err;

    --validate_data()
    BEGIN
        l_log_proc    := pi_control_id;
        putlog(pi_control_id, cn_proc || '|start|params: ' || pi_control_id || ',' || pi_file_id || ',' || pi_file_name);

        insis_sys_v10.insis_context.prepare_session (pi_app         => 'GEN',
                                                       pi_action      => NULL,
                                                       pi_username    => CN_PROCESS_USER, 
                                                       pi_user_role   => 'InsisStaff',
                                                       pi_lang        => NULL,
                                                       pi_country     => NULL);


        v_flag_error  := FALSE;
        v_file_id     := pi_file_id;


/*

    -------------------------------------------
    -- Mandatory details columns validations
    -- -- When not found, stop process for the related master policy
    --
    -- mandatory fields in file
    --
    v_validation := 'mandatory_details';
    v_fields_val := 'Fields: aaaaaa, bbbbbbb, ccccccc, ';
    putlog (cn_proc || '|' || v_validation );

    insert into cust_migration.fa_migr_poller_err(control_id, stag_id, err_code, err_desc, err_type)
    select d.control_id, d.stag_id, v_validation err_code, v_fields_val err_desc, 'fatal' err_type
    from cust_migration.fa_migr_dsgr_mp_pol d
    where control_id = pi_control_id
      and att_status_row <> CN_STAT_REC_ERROR  
      and (
           aaaaaaa is null or 
           bbbbbbb is null or 
           (cccccccc is null and ddddddd in (2009, 2010)) 
           ...
          );

    update_stg_err_mp(pi_control_id, v_validation);

    -- mandatory att fields 
    v_validation := 'mandatory_details_att'; 
    v_fields_val := 'Fields: att_eeeee';
    putlog (cn_proc || '|' || v_validation );

    insert into cust_migration.fa_migr_poller_err(control_id, stag_id, err_code, err_desc, err_type)
    select d.control_id, d.stag_id, v_validation err_code, v_fields_val err_desc, 'fatal' err_type
    from cust_migration.fa_migr_dsgr_mp_pol d
    where control_id = pi_control_id
      and att_status_row <> CN_STAT_REC_ERROR  
      and ( --
           (att_eeeee is null)
          );

    update_stg_err_mp(pi_control_id, v_validation);
    
    
    -------------------------------------------
    -- Fatal Error validations
    -- -- When found, all processes stop
    -------------------------------------------
    
    v_validation := 'unique_dddddd';
    putlog (cn_proc || '|' || v_validation );

    begin
        
        select count(unique ddddd) cant
        into v_cant
        from cust_migration.fa_migr_dsgr_mp_pol
        where control_id = pi_control_id;

        if v_cant > 1 then  
            v_isFatalError := true;
            l_SrvErrMsg := null;
            v_pio_Err := null;

            putlog (cn_proc || '|' || v_validation || '|insr_type: ' ||   v_cant);

            srv_error.SetErrorMsg (l_SrvErrMsg, 'fa_cust_migr_dsgr_mpvalidate_data', 'lpv_policy_issuing_bo.init_val_master_policy_exist', 'Cant:'||v_cant);
            srv_error.SetErrorMsg (l_SrvErrMsg, v_pio_Err);       

            v_errm := v_validation || srv_error.ErrCollection2String(v_pio_Err);

            sys_schema_utils.log_poller_error_process(v_file_id, pi_file_name, 'XLS_INS_OBJ', v_errm, v_gstage);

        end if;

    end;    

    if not v_isFatalError then
        v_validation := 'allowed_insr_type';
        putlog (cn_proc || '|' || v_validation );

        for r_ins_obj_dtl in ( select control_id, stag_id, 
                                               doc_type, doc_number, att_mpi_insr_type, att_mpi_as_is
                                        from cust_migration.fa_migr_dsgr_mp_pol 
                                      where control_id = pi_control_id
                                         and att_status_row <> CN_STAT_REC_ERROR
                                         and ( att_mpi_insr_type is null  or  
                                                  (att_mpi_insr_type || '-' || att_mpi_as_is) not in 
                                                  ('2009-01', '2010-01', '2010-02', '2011-07') ) )
        loop

            l_SrvErrMsg := null;
            v_pio_Err := null;

            putlog ('validate_data|allowed_insr_type: ' ||  r_ins_obj_dtl.stag_id );

            v_isFatalError := true;

            update cust_migration.fa_migr_dsgr_mp_pol 
            set att_status_row = CN_STAT_REC_ERROR
            where control_id = r_ins_obj_dtl.control_id
            and stag_id = r_ins_obj_dtl.stag_id;

            srv_error.SetErrorMsg (l_SrvErrMsg, 'pol_dm.validate_masterPolicyInfo', 'lpv_policy_issuing_bo.init_val_master_policy_exist', r_ins_obj_dtl.att_mpi_insr_type || '-' || r_ins_obj_dtl.att_mpi_as_is);
            srv_error.SetErrorMsg (l_SrvErrMsg, v_pio_Err);       
            v_errm := v_validation || '-' || srv_error.ErrCollection2String(v_pio_Err) ;

            sys_schema_utils.Log_Poller_Error_Process(v_file_id, pi_file_name, 'XLS_INS_OBJ', v_errm, v_gstage);

        end loop;
    end if;
    
    
    ---show fatal error
    if v_isFatalError then
        putlog ('validate_data|isFatal|header' );
        update_stg_err_all(pi_control_id);

        sys_schema_utils.update_poller_process_status (pi_control_id , 'ERROR');
        raise_application_error( -20001, 'generate_job with ID ' || pi_control_id || ' finished with errors.');
    end if;


    --------------------------------------------------------------------------------------------------
    --LOV validations details
    --------------------------------------------------------------------------------------------------

    v_validation := 'rrrrrrrr_lov';
    putlog (cn_proc || '|' || v_validation );

    insert into cust_migration.fa_migr_poller_err(control_id, stag_id, err_code, err_desc, err_type)
    select d.control_id, d.stag_id, v_validation err_code, '' err_desc, 'fatal' err_type
    from cust_migration.fa_migr_dsgr_mp_pol d
    where control_id = pi_control_id
      and att_status_row <> CN_STAT_REC_ERROR  
      and att_mpi_insr_type in (2009, 2010)
      and (rrrrrr is null or rrrrrr not between 1 and 6);

    update_stg_err(pi_control_id, v_validation);

    --

    ...
    

    -------------------------------------------
    -- General error validations
    -------------------------------------------

    --

    v_validation := 'person_older_100y';
    putlog (cn_proc || '|' || v_validation );
    begin
        insert into cust_migration.fa_migr_poller_err(control_id, stag_id, err_code, err_desc, err_type)
        select control_id, stag_id, v_validation err_code, 'People older than 100 years' err_desc, 'fatal' err_type
        from cust_migration.fa_migr_dsgr_mp_pol d 
        where control_id = pi_control_id
          and att_status_row <> CN_STAT_REC_ERROR  
          and att_operation_code in (CN_OPER_EMI, CN_OPER_REN, CN_OPER_INC)
          and (months_between(CN_BIRTH_ENTRY_CALC_DATE, birth_date)/12) > 100; --older than 100 years

    end;

    update_stg_err_mp(pi_control_id, v_validation);

    ...


    -------------------------------------------
    -- Warning validations
    -------------------------------------------

    v_validation := 'diff_worker_type';
    putlog (cn_proc || '|' || v_validation );

    insert into cust_migration.fa_migr_poller_err(control_id, stag_id, err_code, err_desc, err_type)
    select d.control_id, d.stag_id, v_validation err_code, '' err_desc, 'warning' err_type
    from cust_migration.fa_migr_dsgr_mp_pol d
    where d.control_id = pi_control_id
      and d.att_status_row <> CN_STAT_REC_ERROR  
      and d.worker_category <> d.att_mdpi_actual_worker_cat --to-do convertir valores, equivalencia
      and d.att_operation_code in (CN_OPER_EMI, CN_OPER_REN)
      and d.att_mpi_insr_type in (2009, 2010);

--    update_stg_err_mp(pi_control_id, v_validation); --record should not be marked as erroneous

    ...


    ---------------------------------------------------------------------------------
    ---------------------------------------------------------------------------------
    --
    -- Gather and record all errors
    --
    ---------------------------------------------------------------------------------
    putlog (cn_proc || '|gather errors');
    for r_ins_obj_err in (select e.*, d.doc_number
                            from cust_migration.fa_migr_poller_err e
                            left join cust_migration.fa_migr_dsgr_mp_pol d 
                                    on (d.control_id = e.ctrl_id and
                                        d.stag_id = e.stag_id)    
                            where e.ctrl_id = pi_control_id 
                         )
    loop
        l_SrvErrMsg := null;
        v_pio_Err := null;

        putlog ('validate_data|get_policy_by_policy_no: ' ||  r_ins_obj_err.stag_id );

        srv_error.SetErrorMsg (l_SrvErrMsg, 'fa_cust_migr_dsgr_mp.validate_data', 'fa_cust_migr_dsgr_mp'||r_ins_obj_err.ERR_CODE, r_ins_obj_err.ERR_DESC); 
        srv_error.SetErrorMsg (l_SrvErrMsg, v_pio_Err);       

        v_errm := '[' || r_ins_obj_err.err_code|| '] ' || srv_error.ErrCollection2String(v_pio_Err) || '] '|| 'Doc_number [' || r_ins_obj_err.doc_number ||']';

        sys_schema_utils.log_poller_error_process(v_file_id, pi_file_name, 'XLS_INS_OBJ', v_errm, v_gstage);

    end loop;

    commit;

    --
*/
        UPDATE cust_migration.fa_migr_dsgr_mp_pol stg
        SET
            stg.att_status_row = cn_stat_rec_valid
        WHERE
                stg.control_id = pi_control_id
            AND stg.att_status_row <> cn_stat_rec_error;

        COMMIT;
        
        putlog(pi_control_id, cn_proc || '|end|' || SQL%ROWCOUNT);
        v_ret         := TRUE;
        
        RETURN v_ret;
    
    EXCEPTION
        WHEN OTHERS THEN
            srv_error.setsyserrormsg(l_srverrmsg, 'fa_cust_migr_dsgr_mp.validate_data', SQLERRM, SQLCODE);
            srv_error.seterrormsg(l_srverrmsg, v_pio_err);
            putlog(pi_control_id, cn_proc || '|end_error|' || SQLERRM);
            v_ret := FALSE;
            RETURN v_ret;
    END validate_data;

    --
    --updates policy_id created 
    --

    PROCEDURE upd_new_policy_id (
        pi_control_id  fa_migr_dsgr_mp_pol.control_id%TYPE,
        pi_dtl_stg_id  fa_migr_dsgr_mp_pol.stag_id%TYPE,
        pi_policy_id   fa_migr_dsgr_mp_pol.att_policy_id%TYPE
    ) IS
    BEGIN
        UPDATE fa_migr_dsgr_mp_pol
        SET
            att_policy_id = pi_policy_id
        WHERE
                control_id = pi_control_id
            AND stag_id = pi_dtl_stg_id;

    EXCEPTION
        WHEN OTHERS THEN
            putlog(pi_control_id, 'upd_new_policy_id|error|' || SQLERRM);
    END upd_new_policy_id;

    

--
--------------------------------------------------------------------------------
-- Name: process_row
--
-- Type: PROCEDURE
--
-- Subtype: DATA_CHECK
--
-- Status: ACTIVE
--
-- Versioning:
--     La Positiva   23.10.2019  creation
--
-- Purpose: Process  issuance of policy
--
-- Input parameters:
--    pi_control_id 
--    file_id 
--    file_name 
--
-- Output parameters:
--
--------------------------------------------------------------------------------
-- to-do: registrar errores en server. ver si usara parametro de salida(defult) o directo (maybe)

    PROCEDURE process_row (
        pi_control_id       IN   NUMBER,
        pi_fa_migr_pol_row  IN   cust_migration.fa_migr_dsgr_mp_pol%ROWTYPE,
        --po_errs             OUT  srverr
        pio_errmsg          IN OUT  srverr
    ) IS

        cn_proc                         VARCHAR2(100) := 'process_row_' || pi_control_id || '_' || pi_fa_migr_pol_row.stag_id;
        --
        l_outcontext                    srvcontext;
        po_outcontext                   srvcontext;
        l_client_id                     insis_people_v10.p_clients.client_id%TYPE;
--        pio_errmsg                      srverr;
        l_srverrmsg                     insis_sys_v10.srverrmsg;
        l_policy_type                   insis_gen_v10.p_policy_type;
        l_policy_names_type             insis_gen_v10.p_policy_names_type;
        l_eng_policies_type             insis_gen_v10.p_eng_policies_type;
        l_policy_dependent_type         insis_gen_v10.p_policy_type;
        l_policy_dependent_names_type   insis_gen_v10.p_policy_names_type;
        l_policy_condition_issue_exp    insis_gen_v10.p_condition_type;
        
        TYPE objecttype_table IS        TABLE OF VARCHAR2(100) INDEX BY PLS_INTEGER;
        l_obj_type_table                objecttype_table;
        
        l_plan_desc                     VARCHAR2(200);
        l_plan_prev                     VARCHAR2(100);
            
            --TODO: pasar como campo atributo
        l_policy_name                   insis_gen_v10.POLICY.policy_name%TYPE;
        l_policy_id_dependent           insis_gen_v10.POLICY.policy_id%TYPE;
        l_payment_frecuency_code        NUMBER(2);
        l_user_name                     insis_people_v10.POLICY.username%TYPE;
        l_result                        BOOLEAN;
        l_master_policy_id              insis_gen_v10.POLICY.policy_id%TYPE;
        l_agent_id                      insis_people_v10.p_agents.agent_id%TYPE;
        l_agent_id_directos             insis_people_v10.p_agents.agent_id%TYPE;
        l_agent_type                    insis_people_v10.p_agents.agent_type%TYPE;
        l_internal_agent_id             insis_people_v10.p_agents.agent_id%TYPE;
        l_engagement_id                 insis_gen_v10.policy_engagement.engagement_id%TYPE;
        l_insr_begin                    insis_gen_v10.POLICY.insr_begin%TYPE;
        l_office_type                   insis_people_v10.pp_office_type;
        l_office_id                     insis_gen_v10.POLICY.office_id%TYPE;
--        pio_grpi_rec                insis_gen_v10.o_group_ins%rowtype;
--        piquest_questions_rec       insis_sys_v10.quest_questions%rowtype;
        l_insobj_type                   insis_gen_v10.p_insobj_type;
        pio_err_msg                     VARCHAR2(500);
        l_begin_rule                    NUMBER(10);
        l_end_rule                      NUMBER(10);
        l_dimension_policy              VARCHAR2(1);
        l_duration_policy               NUMBER(2);
        l_rule_date                     DATE;
        l_additional_covers_codes       VARCHAR2(500);
        l_additional_covers_types       VARCHAR2(500);
        l_weight                        NUMBER(10);
        l_height                        NUMBER(10, 2);
        l_user_code                     VARCHAR2(20);
        l_application_code              VARCHAR2(20);
        l_processing_number             VARCHAR2(10);
        l_procedure_result              VARCHAR2(100);
        v_insured_id                    insis_people_v10.p_people.man_id%TYPE;
        v_insured_rol_id                insis_gen_v10.o_accinsured.accins_type%TYPE;
        v_insured_relation_shipi_id     insis_people_v10.p_people_relation.rel_id%TYPE;
        v_main_insured_id               insis_people_v10.p_people.man_id%TYPE;
        v_main_insured_rel_id           insis_people_v10.p_people_relation.rel_id%TYPE;
--        l_object_type                   insis_gen_v10.insured_object.object_type%TYPE;
        l_object_id                     insis_gen_v10.insured_object.object_id%TYPE;
        l_ins_obj_id                    insis_gen_v10.insured_object.insured_obj_id%TYPE;
        l_group_ins_obj_id              insis_gen_v10.insured_object.insured_obj_id%TYPE;
        l_sub_group_ins_obj_id          insis_gen_v10.insured_object.insured_obj_id%TYPE;
        l_main_io_ins_obj_id            insis_gen_v10.insured_object.insured_obj_id%TYPE;
        l_object_type_find              insis_gen_v10.insured_object.object_type%TYPE;
        l_accinsured_type               insis_cust.cfglpv_objects_allowance.accinsured_type%TYPE;
        l_dependent_on                  insis_gen_v10.insured_object.object_id%TYPE;
        l_employe_flag                  NUMBER(1);
        l_sales_type                    VARCHAR2(4);
        l_sales_module                  VARCHAR2(4);
        l_insured_group                 VARCHAR2(4);
        l_begin_date                    DATE;
        l_end_date                      DATE;
        l_date_covered                  DATE;
        l_calc_duration                 NUMBER;
        l_calc_dimension                VARCHAR2(1);
        l_tariff_percent                NUMBER;

        --QUESTION
        l_child_quest_id                insis_sys_v10.cfg_quest_depends.child_quest_id%TYPE;
        
        --BENEFICIAR
        v_beneficiar_id                 insis_people_v10.p_people.man_id%TYPE;
        v_premium_share                 insis_gen_v10.policy_participants.premium_share%TYPE;
        v_beneficiar_relation_shipi_id  insis_people_v10.p_people_relation.rel_id%TYPE;        
                     --PAYOR
        v_payor_id                      insis_gen_v10.policy_participants.participant_id%TYPE;
        v_payor_man_id                  insis_gen_v10.policy_participants.man_id%TYPE;
        v_pholder_id                    insis_gen_v10.policy_participants.participant_id%TYPE;
        v_pholder_man_id                insis_gen_v10.policy_participants.man_id%TYPE;
        l_policy_holder_id              insis_people_v10.p_people.man_id%TYPE;
                     --AGENT
        l_collector_id                  insis_people_v10.p_agents.agent_id%TYPE;
                     --BILLING
        l_payment_due_date              insis_gen_v10.policy_engagement_billing.payment_due_date%TYPE;
        l_payment_way                   insis_gen_v10.policy_engagement_billing.payment_way%TYPE;
        l_eng_billing_id                insis_gen_v10.policy_engagement_billing.eng_billing_id%TYPE;
                     --PROFORMA
        l_doc_number                    insis_gen_blc_v10.blc_documents.doc_number%TYPE;
        l_amount                        insis_gen_blc_v10.blc_installments.amount%TYPE;
        l_pp_client_rec                 insis_people_v10.pp_client_type;
        l_pp_client_arr                 insis_people_v10.pp_client_table;
        l_tech_branch                   insis_gen_v10.POLICY.attr1%TYPE;
        l_sbs_code                      insis_gen_v10.POLICY.attr2%TYPE;
        l_staff_id                      insis_gen_v10.POLICY.staff_id%TYPE;
        
        l_curr_plan_id                  PLS_INTEGER;
    
        CURSOR c_staff_id
            IS
        SELECT A.staff_id
          FROM insis_people_v10.p_staff A, insis_people_v10.p_people b
         WHERE A.man_id = b.man_id
           AND b.NAME = CN_POLICY_USER; --Cambio de usuario estaba seteado con CN_PROCESS_USER: INSIS_GEN_V10
        --
        
        CURSOR c_object_types(
            pi_insr_type    insis_cust.cfglpv_groups_allowance.insr_type%TYPE, 
            pi_as_is        insis_cust.cfglpv_groups_allowance.as_is_product%TYPE) IS 
        SELECT
              gr.object_type, 
              CASE WHEN gr.ref_group_object_type IS NULL THEN 1 ELSE 0 END is_group,
              --names are translated to spanish
                REPLACE(
                    REPLACE(
                        REPLACE(typ.NAME,
                                'Main',     'Principal'), 
                            'Additional','Adicional'),
                        'Addition','Adicional') NAME
          FROM
              insis_cust.cfglpv_groups_allowance gr
              INNER JOIN insis_gen_v10.hst_object_type typ ON ( gr.object_type = typ.ID )
          WHERE
                  gr.insr_type = pi_insr_type
            AND gr.as_is_product = pi_as_is
            --exclude root group record
            AND NOT (gr.mandatory = 'N' AND gr.ref_group_object_type IS NULL)
            ORDER BY object_type;
            
        l_object_types_rec  c_object_types%ROWTYPE;

        TYPE agent_id_table IS TABLE OF NUMBER INDEX BY VARCHAR2(10);
        l_agent_id_table        agent_id_table; 
        l_agent_role_type       VARCHAR2(10);
        
        --unique plans by policy_no in file
        CURSOR c_fa_migr_cov_plans(
            pi_c_control_id  fa_migr_dsgr_mp_cov.control_id%TYPE,
            pi_c_policy_no   fa_migr_dsgr_mp_cov.policy_no%TYPE) 
        IS 
            SELECT UNIQUE CV.control_id, CV.policy_no, CV.plan_name, CV.subplan_name,
                          CV.plan_max_age, CV.plan_min_age, CV.max_outstand, CV.min_outstand, 
                          CV.max_loan_dur, CV.min_loan_dur
            FROM fa_migr_dsgr_mp_cov CV
           WHERE CV.control_id = pi_c_control_id
             AND CV.policy_no = pi_c_policy_no
           ORDER BY CV.plan_name, CASE WHEN lower(CV.subplan_name) = 'titular' THEN 1 ELSE 2 END;
        
        l_fa_migr_cov_plans     c_fa_migr_cov_plans%ROWTYPE;

        
        -- TODO: pasar a UTILS
        --------------------------------------------------------------------------------
        -- Name: get_client_id_by_egn
        --------------------------------------------------------------------------------
        -- Purpose: Get client_id from pid
        --------------------------------------------------------------------------------
    
        FUNCTION get_client_id_by_egn (
            pi_egn insis_people_v10.p_people.egn%TYPE
        ) RETURN insis_people_v10.p_clients.client_id%TYPE AS
            l_client_id insis_people_v10.p_clients.client_id%TYPE;
        BEGIN
            BEGIN
                SELECT
                    c.client_id
                INTO l_client_id
                FROM
                    insis_people_v10.p_people P
                    INNER JOIN insis_people_v10.p_clients c ON (c.man_id = P.man_id)
                WHERE
                    P.egn = pi_egn;
    
            EXCEPTION
                WHEN OTHERS THEN
                    l_client_id := NULL;
            END;
    
            RETURN l_client_id;
        END get_client_id_by_egn;
        

        
        --
        -- get_sbs_techbr. get data from product
        --
        PROCEDURE get_sbs_techbr (
            pi_insr_type    IN   insis_cust.cfglpv_policy_techbranch_sbs.insr_type%TYPE,
            pi_as_is        IN   insis_cust.cfglpv_policy_techbranch_sbs.as_is_product%TYPE,
            po_tech_branch  OUT  insis_cust.cfglpv_policy_techbranch_sbs.technical_branch%TYPE,
            po_sbs_code     OUT  insis_cust.cfglpv_policy_techbranch_sbs.sbs_code%TYPE
        ) AS
        BEGIN
            BEGIN
                SELECT
                    technical_branch,
                    sbs_code
                INTO
                    po_tech_branch,
                    po_sbs_code
                FROM
                    insis_cust.cfglpv_policy_techbranch_sbs
                WHERE
                        insr_type = pi_insr_type
                    AND as_is_product = pi_as_is;

            EXCEPTION
                WHEN OTHERS THEN
                    po_tech_branch  := NULL;
                    po_sbs_code     := NULL;
                    putlog(pi_fa_migr_pol_row.control_id, '--get_sbs_techbr.err:' || SQLERRM);
            END;
        END get_sbs_techbr;

        --
        -- yn_to_num: convert yes/no values to number
        --

        FUNCTION yn_to_num (
            pi_yn       VARCHAR2,
            pi_val_yes  PLS_INTEGER,
            pi_val_no   PLS_INTEGER
        ) RETURN VARCHAR2 AS
        BEGIN
            RETURN
                CASE
                    WHEN pi_yn = 'Y' THEN
                        pi_val_yes
                    ELSE pi_val_no
                END;
        END yn_to_num; 


        --
        -- get_role_by_accins 
        --
        FUNCTION get_role_by_accins (
            pi_accins PLS_INTEGER
        ) RETURN VARCHAR2 AS
            v_role VARCHAR2(20);
        BEGIN
            IF pi_accins = 1 THEN
                v_role := 'Principal';
            ELSIF pi_accins = 2 THEN
                v_role := 'Conyuge';
            ELSIF pi_accins = 3 THEN
                v_role := 'Hijo';
            ELSIF pi_accins = 4 THEN
                v_role := 'Adicional';
            ELSIF pi_accins = 5 THEN
                v_role := 'Familiar';
            ELSIF pi_accins = 6 THEN
                v_role := 'Padres';
            ELSE
                v_role := NULL;
            END IF;

            RETURN v_role;
        END get_role_by_accins;

        --
        -- get_rel_id_by_desc
        --

        FUNCTION get_rel_id_by_desc (
            pi_rel_desc VARCHAR2
        ) RETURN insis_people_v10.ht_people_relation.rel_id%TYPE AS
            v_ret insis_people_v10.ht_people_relation.rel_id%TYPE;
        BEGIN
            BEGIN
                SELECT
                    rel.rel_id
                INTO v_ret
                FROM
                    insis_people_v10.ht_people_relation    rel
                    LEFT JOIN insis_cust.cfg_nom_language_table      lan ON ( lan.ID = rel.rel_id )
                WHERE
                        1 = 1
                    AND lan.table_name LIKE '%HT_PEOPLE_RELATION%'
                    AND NAME LIKE '%' || upper(pi_rel_desc) || '%';

            EXCEPTION
                WHEN OTHERS THEN
                    v_ret := NULL;
            END;

            RETURN v_ret;
        END get_rel_id_by_desc;

        --
        -- update_conditions: update policy_conditions
        --

        PROCEDURE update_conditions (
            pi_control_id  IN      PLS_INTEGER,
            pi_stag_id     IN      PLS_INTEGER,
            pi_cond_type   IN      insis_gen_v10.policy_conditions.cond_type%TYPE,
            pi_cond_dim    IN      insis_gen_v10.policy_conditions.cond_dimension%TYPE,
            pi_cond_val    IN      insis_gen_v10.policy_conditions.cond_value%TYPE,
            pi_policy_id   IN      insis_gen_v10.policy_conditions.policy_id%TYPE,
            pi_annex_id    IN      insis_gen_v10.policy_conditions.annex_id%TYPE,
            pio_errmsg     IN OUT  srverr
        ) AS
        BEGIN
            putlog(pi_control_id, '--update_conditions:' || pi_cond_type);
            UPDATE insis_gen_v10.policy_conditions
            SET
                cond_dimension = pi_cond_dim,
                cond_value = pi_cond_val
            WHERE
                    policy_id = pi_policy_id
                AND annex_id = pi_annex_id
                AND cond_type = pi_cond_type;

        EXCEPTION
            WHEN OTHERS THEN
                putlog(pi_control_id, '--update_conditions.err:' || SQLERRM);
                srv_error_set('update_conditions', 'SYSERROR', SQLERRM, pio_errmsg);
        END update_conditions;


        --todo: pasar utils
        --update question for policy
        --

        PROCEDURE update_quest (
            pi_quest_code    IN      insis_sys_v10.quest_questions.quest_id%TYPE,
            pi_quest_answer  IN      insis_sys_v10.quest_questions.quest_answer%TYPE,
            pi_policy_id     IN      insis_sys_v10.quest_questions.policy_id%TYPE,
            pi_annex_id      IN      insis_sys_v10.quest_questions.annex_id%TYPE,
            pio_outcontext   IN OUT  srvcontext,
            pio_errmsg       IN OUT  srverr
        ) AS
        BEGIN
            putlog(pi_control_id, '--GET_POL_QUEST|' || pi_policy_id || ':' || pi_quest_code || ':' || pi_quest_answer);

            IF pi_policy_id IS NULL THEN
                RETURN;
            END IF;
        
            -- GET_POL_QUEST
            insis_sys_v10.srv_context.setcontextattrnumber(pio_outcontext, 'POLICY_ID', insis_sys_v10.srv_context.integers_format,
            pi_policy_id);

            insis_sys_v10.srv_context.setcontextattrnumber(pio_outcontext, 'ANNEX_ID', insis_sys_v10.srv_context.integers_format,
            pi_annex_id);

            insis_sys_v10.srv_context.setcontextattrchar(pio_outcontext, 'QUEST_CODE', pi_quest_code);
            insis_sys_v10.srv_events.sysevent('GET_POL_QUEST', pio_outcontext, pio_outcontext, pio_errmsg);
            IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
                putlog(pi_control_id, '--GET_POL_QUEST.err:' || srv_error.errcollection2string(pio_errmsg));
                RETURN;
            END IF;        
        
            --UPD_QUEST

            putlog(pi_control_id, '--UPD_QUEST');
            insis_sys_v10.srv_context.setcontextattrnumber(pio_outcontext, 'ID', insis_sys_v10.srv_context.integers_format, insis_sys_v10.
            srv_question_data.gquestionrecord.ID);

            insis_sys_v10.srv_context.setcontextattrchar(pio_outcontext, 'QUEST_ANSWER', pi_quest_answer);
            insis_sys_v10.srv_events.sysevent('UPD_QUEST', pio_outcontext, pio_outcontext, pio_errmsg);
            IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
                putlog(pi_control_id, '--UPD_QUEST.err:' || srv_error.errcollection2string(pio_errmsg));
                RETURN;
            END IF;

        END update_quest;

        --todo: pasar a utils
        -- update question for insured
        --

        PROCEDURE update_quest (
            pi_quest_code    IN      insis_sys_v10.quest_questions.quest_id%TYPE,
            pi_quest_answer  IN      insis_sys_v10.quest_questions.quest_answer%TYPE,
            pi_insured_id    IN      insis_sys_v10.quest_questions.insured_id%TYPE,
            pio_outcontext   IN OUT  srvcontext,
            pio_errmsg       IN OUT  srverr
        ) AS
        BEGIN
            putlog(pi_control_id, '--GET_INSURED_QUEST|' || pi_insured_id || ':' || pi_quest_code || ':' || pi_quest_answer);

            IF pi_insured_id IS NULL THEN
                RETURN;
            END IF;
            
            --GET_INSURED_QUEST
            insis_sys_v10.srv_prm_quest.sinsuredobjid(pio_outcontext, pi_insured_id);
            insis_sys_v10.srv_prm_quest.squestcode(pio_outcontext, pi_quest_code);
            insis_sys_v10.srv_events.sysevent('GET_INSURED_QUEST', pio_outcontext, pio_outcontext, pio_errmsg);
            IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
                putlog(pi_control_id, '--GET_POL_QUEST.err:' || srv_error.errcollection2string(pio_errmsg));
                RETURN;
            END IF;        
        
            --UPD_QUEST

            putlog(pi_control_id, '--UPD_QUEST');
            insis_sys_v10.srv_context.setcontextattrnumber(pio_outcontext, 'ID', insis_sys_v10.srv_context.integers_format, insis_sys_v10.
            srv_question_data.gquestionrecord.ID);

            insis_sys_v10.srv_context.setcontextattrchar(pio_outcontext, 'QUEST_ANSWER', pi_quest_answer);
            insis_sys_v10.srv_events.sysevent('UPD_QUEST', pio_outcontext, pio_outcontext, pio_errmsg);
            IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
                putlog(pi_control_id, '--UPD_QUEST.err:' || srv_error.errcollection2string(pio_errmsg));
                RETURN;
            END IF;

        END update_quest;

        -- 
        -- upd_people_relation
        -- 

        PROCEDURE upd_people_relation (
            pi_part1_man_id  IN      insis_people_v10.p_people_relation.part1_id%TYPE,
            pi_part2_man_id  IN      insis_people_v10.p_people_relation.part1_id%TYPE,
            pi_relation      IN      insis_people_v10.p_people_relation.rel_id%TYPE,
            pi_insr_begin    IN      insis_people_v10.p_people_relation.valid_from%TYPE,
            pi_err           IN OUT  srverr
        ) AS

            l_reg_exist_rel  NUMBER(2);
            l_outcontext     srvcontext;
            l_valid_from     DATE;
            v_old_relation   insis_people_v10.p_people_relation.rel_id%TYPE;
        BEGIN
            SELECT
                COUNT(*)
            INTO l_reg_exist_rel
            FROM
                insis_people_v10.p_people_relation
            WHERE
                    part1_id = pi_part1_man_id
                AND part2_id = pi_part2_man_id
                AND ( pi_insr_begin BETWEEN valid_from AND valid_to
                      OR valid_to IS NULL );

            IF l_reg_exist_rel = 0 THEN
                putlog(pi_control_id, cn_proc || '|Record_Participant|--INS_RELATION');
                srv_context.setcontextattrnumber(l_outcontext, 'PART1_ID', srv_context.integers_format, pi_part1_man_id);
                srv_context.setcontextattrnumber(l_outcontext, 'PART2_ID', srv_context.integers_format, pi_part2_man_id);
                srv_context.setcontextattrnumber(l_outcontext, 'REL_ID', srv_context.integers_format, pi_relation);
                srv_context.setcontextattrdate(l_outcontext, 'VALID_FROM', srv_context.date_format, pi_insr_begin);
                srv_context.setcontextattrdate(l_outcontext, 'VALID_TO', srv_context.date_format, NULL);
                insis_sys_v10.srv_events.sysevent('INS_RELATION', l_outcontext, l_outcontext, pi_err);
                IF NOT srv_error.rqstatus(pi_err) THEN
                    RETURN;
                END IF;
            ELSE
                
                --check if start of relation need to be updated (backdate )
                putlog(pi_control_id, cn_proc || '|Record_Participant|--Select Relation 2');
                SELECT
                    valid_from,
                    rel_id
                INTO
                    l_valid_from,
                    v_old_relation
                FROM
                    insis_people_v10.p_people_relation
                WHERE
                        part1_id = pi_part1_man_id
                    AND part2_id = pi_part2_man_id;

                IF l_valid_from > pi_insr_begin THEN
                    putlog(pi_control_id, cn_proc || '|Record_Participant|--UPD_RELATION');
                    srv_context.setcontextattrnumber(l_outcontext, 'PART1_ID', srv_context.integers_format, pi_part1_man_id);
                    srv_context.setcontextattrnumber(l_outcontext, 'PART2_ID', srv_context.integers_format, pi_part2_man_id);
                    srv_context.setcontextattrnumber(l_outcontext, 'REL_ID', srv_context.integers_format, v_old_relation);
                    srv_context.setcontextattrdate(l_outcontext, 'VALID_FROM', srv_context.date_format, pi_insr_begin);
                    srv_context.setcontextattrdate(l_outcontext, 'VALID_TO', srv_context.date_format, NULL);
                    insis_sys_v10.srv_events.sysevent('UPD_RELATION', l_outcontext, l_outcontext, pi_err);
                    IF NOT srv_error.rqstatus(pi_err) THEN
                        RETURN;
                    END IF;
                END IF;

            END IF;

        END upd_people_relation;
        
        --
        -- Record_Participant: Calls events for participant creation
        --
        PROCEDURE Record_Participant(
            pi_proc              IN varchar2,
            pi_policy_id         fa_migr_dsgr_mp_pol.att_policy_id%TYPE,
            pi_annex_id          fa_migr_dsgr_mp_pol.att_policy_id%TYPE, 
            pi_part_role         insis_gen_v10.policy_participants.particpant_role%TYPE,   
            pi_part_man_id       fa_migr_dsgr_mp_pol.att_pholder_manid%TYPE,
            pi_insr_begin        date,
            pi_insr_end          date,
--            pi_part_perc         fa_migr_spf_dp_par.benef_part_perc%TYPE DEFAULT 100,
            pi_part_perc         IN NUMBER DEFAULT 100,
--            pi_ins_upd           varchar2,   
--            pi_main_man_id       fa_migr_spf_dp_pol.att_main_io_man_id%type,
--            pi_benef_rel_main    insis_people_v10.ht_people_relation.rel_id%type, 
            pi_context           IN OUT srvcontext,
            pi_err               IN OUT srverr
            )
        IS
            cn_proc             varchar2(100) := pi_proc;
            l_outcontext        srvcontext;
            l_result            BOOLEAN;
            l_insured_obj_type  PLS_INTEGER;
            l_accins_type       PLS_INTEGER;
            l_plan_insured_obj_id insis_gen_v10.insured_object.insured_obj_id%TYPE;
        
        BEGIN 
            
            putlog(pi_control_id, cn_proc ||'|Record_Participant|start|'|| pi_part_role || '-' || pi_part_man_id);

            IF pi_part_man_id IS NULL THEN
                RETURN;
            END IF;    
            
            putlog(pi_control_id, cn_proc ||'|Record_Participant|--CHECK_RECORD_POLICY_PARTS.'|| pi_part_role);
            srv_context.setcontextattrchar(l_outcontext, 'PARTICPANT_ROLE', pi_part_role); --'BENEFICENT' , 'ADDBENPROV'
            srv_context.setcontextattrchar(l_outcontext, 'LANGUAGE', 'SPANISH');
            srv_context.setcontextattrchar(l_outcontext, 'STATUS', 'ACTIVE');
            srv_context.setcontextattrnumber(l_outcontext, 'POLICY_ID', srv_context.integers_format, pi_policy_id);
            srv_context.setcontextattrnumber(l_outcontext, 'ANNEX_ID', srv_context.integers_format, pi_annex_id);

            srv_context.setcontextattrnumber(l_outcontext, 'MAN_ID', srv_context.integers_format, pi_part_man_id);
            srv_context.setcontextattrdate(l_outcontext, 'VALID_FROM', srv_context.date_format, pi_insr_begin);
            srv_context.setcontextattrnumber(l_outcontext, 'PREMIUM_SHARE', srv_context.integers_format, pi_part_perc);
            srv_context.setcontextattrdate(l_outcontext, 'VALID_TO', srv_context.date_format, pi_insr_end);
                
            -----------------------------------------------------------------------------------------------
            --CHECK_RECORD_POLICY_PARTS
            -----------------------------------------------------------------------------------------------
            --INSIS_GEN_V10.SRV_POLICY_DATA.GPOLICYPARTICIPANTRECORD := NULL;
            --INSIS_GEN_V10.SRV_POLICY_DATA.GPOLICYPARTICIPANTTABLE:= NULL;   
            srv_context.setcontextattrnumber(l_outcontext, 'PARTICIPANT_ID', srv_context.integers_format, NULL);
            
            insis_gen_v10.srv_events.sysEvent( 'CHECK_RECORD_POLICY_PARTS', l_outcontext, l_outcontext, pi_err );

            IF NOT srv_error.rqstatus(pi_err) THEN
                RETURN;
            END IF;
            
            -----------------------------------------------------------------------------------------------
            --INS_POLICY_PARTICIPANS
            -----------------------------------------------------------------------------------------------
            putlog(pi_control_id, cn_proc ||'|Record_Participant|--INS_POLICY_PARTICIPANS');
            insis_gen_v10.srv_events.sysEvent( 'INS_POLICY_PARTICIPANS', l_outcontext, l_outcontext, pi_err );

            IF NOT srv_error.rqstatus(pi_err) THEN
                RETURN;
            END IF;

            -----------------------------------------------------------------------------------------------
            --CONSISTENCY_POLICY_PARTS
            -----------------------------------------------------------------------------------------------
            putlog(pi_control_id, cn_proc ||'|Record_Participant|--CONSISTENCY_POLICY_PARTS');
            srv_context.setcontextattrnumber(l_outcontext, 'PARTICIPANT_ID', srv_context.integers_format, insis_gen_v10.srv_policy_data.gpolicyparticipantrecord.participant_id);
            insis_gen_v10.srv_events.sysEvent( 'CONSISTENCY_POLICY_PARTS', l_outcontext, l_outcontext, pi_err );


            IF NOT srv_error.rqstatus(pi_err) THEN
                RETURN;
            END IF;

            -----------------------------------------------------------------------------------------------
            
            -----------------------------------------------------------------------------------------
            --INS_RELATION
            -----------------------------------------------------------------------------------------
--            IF pi_benef_rel_main <> 0 THEN
--                IF pi_main_man_id <> pi_benef_man_id THEN
--                    putlog(cn_proc ||'|Record_Participant|--Select Relation');
--                    
--                    upd_people_relation(pi_main_man_id, pi_benef_man_id, pi_benef_rel_main, pi_insr_begin, pi_err);
--                                        
--                    IF NOT srv_error.rqstatus(pi_err) THEN
--                        return;
--                    END IF;
--
--                END IF;
--            END IF;
            
            
            putlog(pi_control_id, cn_proc ||'|Record_Participant|end');
        EXCEPTION 
            WHEN others THEN
                putlog(pi_control_id, cn_proc ||'|Record_Participant|end_err|'  || SQLERRM);
                
        END Record_Participant;

        PROCEDURE ins_pol_special_comm(
            pi_comm_apply          fa_migr_dsgr_mp_cov.mark_c_spec_comm_type%TYPE, 
            pi_plan_name           fa_migr_dsgr_mp_cov.plan_name%TYPE, --agregar plan_name
            pi_subplan_name        fa_migr_dsgr_mp_cov.subplan_name%TYPE,
            pio_errmsg_loc  IN OUT srverr) IS
            
            l_commval_type                  insis_cust.lpv_commval_obj_lvl_type;
            
            CURSOR c_fa_commtype(
                pi_control_id       cust_migration.fa_migr_dsgr_mp_pol.control_id%TYPE, 
                pi_policy_no        cust_migration.fa_migr_dsgr_mp_pol.policy_no%TYPE,
                pi_plan_name        cust_migration.fa_migr_dsgr_mp_cov.plan_name%TYPE,
                pi_subplan_name     cust_migration.fa_migr_dsgr_mp_cov.subplan_name%TYPE) IS

                SELECT
                    'MARKCO' comm_type,
                    subplan_name,
                    cover_type,
                    mark_gu_coll_spec_comm_type objcov_type,
                    mark_gu_coll_spec_comm  comm_value,
                    mark_gu_coll_spec_dim   comm_dim
                FROM
                    fa_migr_dsgr_mp_cov
                WHERE
                        control_id = pi_control_id
                    AND policy_no = pi_policy_no
                    AND (plan_name = pi_plan_name OR pi_plan_name IS NULL)
                    AND (subplan_name = pi_subplan_name OR pi_subplan_name IS NULL) 
                    AND mark_gu_coll_spec_comm_type LIKE pi_comm_apply ||'%'
                UNION
                SELECT
                    'MARKAC',
                    subplan_name,
                    cover_type,
                    mark_gu_adq_spec_comm_type,
                    mark_gu_adq_spec_comm  comm,
                    mark_gu_adq_spec_dim   dim
                FROM
                    fa_migr_dsgr_mp_cov
                WHERE
                        control_id = pi_control_id
                    AND policy_no = pi_policy_no
                    AND (plan_name = pi_plan_name OR pi_plan_name IS NULL)
                    AND (subplan_name = pi_subplan_name OR pi_subplan_name IS NULL)
                    AND mark_gu_adq_spec_comm_type LIKE pi_comm_apply ||'%'
                UNION
                SELECT
                    'MARK',
                    subplan_name,
                    cover_type,
                    mark_c_spec_comm_type,
                    mark_c_spec_comm  comm,
                    mark_c_spec_dim   dim
                FROM
                    fa_migr_dsgr_mp_cov
                WHERE
                        control_id = pi_control_id
                    AND policy_no = pi_policy_no
                    AND (plan_name = pi_plan_name OR pi_plan_name IS NULL)
                    AND (subplan_name = pi_subplan_name OR pi_subplan_name IS NULL)
                    AND mark_c_spec_comm_type LIKE pi_comm_apply ||'%'
                    ;
            
            l_fa_commtype_rec   c_fa_commtype%ROWTYPE;

            
        BEGIN
            putlog(pi_fa_migr_pol_row.control_id,'ins_pol_special_comm_rate/' || pi_fa_migr_pol_row.control_id || '/' || pi_fa_migr_pol_row.policy_no || '/' || pi_plan_name || '/' || pi_subplan_name );
            
            OPEN c_fa_commtype(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.policy_no, pi_plan_name, pi_subplan_name);
            LOOP
                FETCH c_fa_commtype INTO l_fa_commtype_rec;
                exit WHEN c_fa_commtype%NOTFOUND;
                
                l_commval_type := NEW insis_cust.lpv_commval_obj_lvl_type();
                l_commval_type.policy_id := l_master_policy_id;
                l_commval_type.annex_id  := insis_gen_v10.gvar_pas.def_annx_id;
                l_commval_type.insured_obj_id := CASE WHEN l_fa_commtype_rec.objcov_type = 'Cobertura' THEN NULL ELSE l_ins_obj_id END;
                l_commval_type.cover_type := CASE WHEN l_fa_commtype_rec.objcov_type = 'Objeto' THEN NULL ELSE l_fa_commtype_rec.cover_type END;
                l_commval_type.comm_type  := l_fa_commtype_rec.comm_type;
                l_commval_type.comm_value := l_fa_commtype_rec.comm_value;
                l_commval_type.comm_currency  := pi_fa_migr_pol_row.currency;
                l_commval_type.comm_dimension := CASE WHEN l_fa_commtype_rec.comm_dim = 'Porcentaje' THEN 'P' ELSE 'V' END;
                l_commval_type.valid_from := l_begin_date;
                l_commval_type.valid_to   := l_end_date;  
               
                
    --                                 --
    --                                 --LPV-2671 check if the record already exists
    --                                 FOR i IN (SELECT * FROM lpv_commval_obj_lvl
    --                                           WHERE policy_id  = l_cv_type.policy_id
    --                                             AND annex_id   = l_cv_type.annex_id
    --                                             AND insured_obj_id = l_cv_type.insured_obj_id
    --                                             AND cover_type = l_cv_type.cover_type
    --                                             AND comm_type  = l_cv_type.comm_type)
    --                                 LOOP
    --                                    l_insert := FALSE;
    --                                 END LOOP;
    
    --                                 IF l_insert --LPV-2671
    --                                 THEN
                        IF NOT l_commval_type.InsertCommObjLvl ( pio_errmsg_loc )
                        THEN
                            putlog(pi_fa_migr_pol_row.control_id,'pol_special_comm.err');
                            exit; --Srv_Error.SetErrorMsg ( l_SrvErrMsg, pio_errmsg );
                        ELSE
    --                                            putlog(pi_fa_migr_pol_row.control_id,'pol_special_comm.ins:'||l_commval_type.comm_type||','||l_commval_type.cover_type);
                            NULL;
                        END IF;
    --                                 END IF;
            END LOOP;
            CLOSE c_fa_commtype;

        END ins_pol_special_comm;

    BEGIN
        l_log_proc := pi_fa_migr_pol_row.control_id || '-' || pi_fa_migr_pol_row.stag_id;
        putlog(pi_fa_migr_pol_row.control_id, 'process_row|start|' || pi_fa_migr_pol_row.policy_no);

        EXECUTE IMMEDIATE 'alter session set NLS_NUMERIC_CHARACTERS = ''.,''';
        
        insis_sys_v10.insis_context.prepare_session (pi_app         => 'GEN',
                                                       pi_action      => NULL,
                                                       pi_username    => CN_PROCESS_USER,
                                                       pi_user_role   => 'InsisStaff',
                                                       pi_lang        => NULL,
                                                       pi_country     => NULL);



        l_outcontext  := srvcontext();
        
        --todo: cargar client_id en columna ATT
        l_client_id := get_client_id_by_egn(pi_fa_migr_pol_row.pholder_pid);
        
        putlog(pi_fa_migr_pol_row.control_id, 'l_client_id:'||l_client_id); 

        
-- todo: pasar agent_type a campo att_internal_agent_type (nuevo)
        l_agent_id := pi_fa_migr_pol_row.att_internal_agent_id;
--        l_agent_type := pi_fa_migr_pol_row.att_internal_agent_type;
--
--        putlog(pi_fa_migr_pol_row.control_id, 'select agent_id'); 
--        BEGIN
--            SELECT
--                agent_id, agent_type
--            INTO l_agent_id, l_agent_type
--            FROM
--                insis_people_v10.p_agents
--            WHERE
--                agent_id = pi_fa_migr_pol_row.att_internal_agent_id
--            ;
--
--        EXCEPTION
--            WHEN OTHERS THEN
--                putlog(pi_fa_migr_pol_row.control_id, 'select agent_id.err:'||sqlerrm); 
--                srv_error_set('select agent_id', 'InsrDurValidate_Agent', sqlerrm, pio_errmsg);
--                return;
--        END;
--        putlog(pi_fa_migr_pol_row.control_id, 'agent_id, type:' || l_agent_id || ',' || l_agent_type); 
--        putlog(pi_fa_migr_pol_row.control_id, 'select office_id');

        
        l_office_type := NEW insis_people_v10.pp_office_type( CASE WHEN length(pi_fa_migr_pol_row.office_lp_no)<2 THEN lpad(pi_fa_migr_pol_row.office_lp_no, 2,'0')
                                                                    ELSE pi_fa_migr_pol_row.office_lp_no
                                                              END); --todo: corregir lectura lpad
        IF l_office_type IS NOT NULL AND 
           l_office_type.office_id IS NOT NULL THEN
--            putlog(pi_fa_migr_pol_row.control_id, 'office_id:'||l_office_type.office_id); 
            l_office_id := l_office_type.office_id; --todo: asignar nuevo campo: r_fa_migr_spf_dp_pol.att_office_id
        END IF;

        ----###
        putlog(pi_fa_migr_pol_row.control_id, '--CREATE_ENGAGEMENT');
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ENGAGEMENT_ID', insis_sys_v10.srv_context.integers_format, NULL);
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'CLIENT_ID', insis_sys_v10.srv_context.integers_format, l_client_id);
        insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'ENGAGEMENT_STAGE', insis_gen_v10.gvar_pas.at_appl);
        insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'ENGAGEMENT_TYPE', insis_gen_v10.gvar_pas.eng_type_engagement);
        insis_sys_v10.srv_events.sysevent('CREATE_ENGAGEMENT', l_outcontext, l_outcontext, pio_errmsg);
        IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
            putlog(pi_fa_migr_pol_row.control_id, '--CREATE_ENGAGEMENT.err:' || srv_error.errcollection2string(pio_errmsg));
            RETURN;
        END IF;

        insis_sys_v10.srv_context.getcontextattrnumber(l_outcontext, 'ENGAGEMENT_ID', l_engagement_id);

        ----###
        putlog(pi_fa_migr_pol_row.control_id, '--CREATE_ENG_POLICY');
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ENGAGEMENT_ID', insis_sys_v10.srv_context.integers_format, l_engagement_id);
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'INSR_TYPE', insis_sys_v10.srv_context.integers_format, CN_INSR_TYPE);
        insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'POLICY_TYPE', insis_gen_v10.gvar_pas.engpoltype_master);
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_ID_ORG', insis_sys_v10.srv_context.integers_format, NULL);
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_STAGE', insis_sys_v10.srv_context.integers_format, insis_gen_v10.gvar_pas.define_applprep_state);
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'AGENT_ID', insis_sys_v10.srv_context.integers_format,l_agent_id);

--        --In case of "Asesor" agent, it is necesary to include "Directos" internal agent
--        --todo:usar contantes
--
--        IF l_agent_type = 5 THEN
--            putlog(pi_fa_migr_pol_row.control_id, 'select agent_id 1412');
--            BEGIN
--                SELECT
--                    agent_id
--                INTO l_agent_id_directos
--                FROM
--                    insis_people_v10.p_agents
--                WHERE
--                    agent_no = '1412';
--
--            EXCEPTION
--                WHEN OTHERS THEN
--                    putlog(pi_fa_migr_pol_row.control_id, 'agent_id1412.err:' || sqlerrm);
--                    srv_error_set('select agent_id_1412', 'InsrDurValidate_Agent', sqlerrm, pio_errmsg);
--                    return;
--            END;
--
--            insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'AGENT_ID', insis_sys_v10.srv_context.integers_format, l_agent_id_directos);--DIRECTOS            
--
--        ELSE
--            insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'AGENT_ID', insis_sys_v10.srv_context.integers_format, l_agent_id);
--        END IF;
        insis_sys_v10.srv_events.sysevent('CREATE_ENG_POLICY', l_outcontext, l_outcontext, pio_errmsg);

        IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
            putlog(pi_fa_migr_pol_row.control_id, 'CREATE_ENG_POLICY.err:' || srv_error.errcollection2string(pio_errmsg));
            RETURN;
        END IF;

        insis_sys_v10.srv_context.getcontextattrnumber(l_outcontext, 'POLICY_ID', l_master_policy_id);
        putlog(pi_fa_migr_pol_row.control_id, 'POLICY_ID:' || l_master_policy_id); 


        --todo: temporary update ?
        upd_new_policy_id(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, l_master_policy_id);
        
       
----        putlogcontext(pi_fa_migr_pol_row.control_id, l_outcontext);
--        

        l_begin_date              := tdate(pi_fa_migr_pol_row.insr_begin); --+ 0.5;
        l_end_date                := tdate(pi_fa_migr_pol_row.insr_end); -- + 0.5 - 1/24/60/60;
--        l_date_covered   := tdate(pi_fa_migr_pol_row.coverdate);

        
        putlog(pi_fa_migr_pol_row.control_id, 'Creating agents');

--        putlog(pi_fa_migr_pol_row.control_id, 'Setting Agent types');

        l_agent_id_table('BROK')    := pi_fa_migr_pol_row.att_broker_agent_id;
        l_agent_id_table('MARK')    := pi_fa_migr_pol_row.att_mark_c_agent_id;
        l_agent_id_table('MARKCO')  := pi_fa_migr_pol_row.att_mark_gu_coll_agent_id;
        l_agent_id_table('MARKAC')  := pi_fa_migr_pol_row.att_mark_gu_acq_agent_id;
        l_agent_id_table('MARKPS')  := pi_fa_migr_pol_row.att_mark_ps_agent_id;
        
        --Uses WHILE ..LOOP because FOR .. LOOP gives error        
        l_agent_role_type := l_agent_id_table.first;
        WHILE l_agent_role_type IS NOT NULL
        LOOP
        
--            putlog(pi_fa_migr_pol_row.control_id, '--l_agent_role_type.'||l_agent_role_type);
        
            l_agent_id := l_agent_id_table(l_agent_role_type);
            
            IF l_agent_id IS NOT NULL THEN
--                putlog(pi_fa_migr_pol_row.control_id, '--INS_POLICY_AGENTS.'||l_agent_role_type);
               
                insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_AGENT_ID', insis_sys_v10.srv_context.integers_format, NULL);
                insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'AGENT_ID', insis_sys_v10.srv_context.integers_format, l_agent_id);
                insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ANNEX_ID', insis_sys_v10.srv_context.integers_format, insis_gen_v10.gvar_pas.def_annx_id);
                insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'AGENT_ROLE', l_agent_role_type);
                insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'COMM_SHARE', insis_sys_v10.srv_context.integers_format, 100);
                insis_sys_v10.srv_context.setcontextattrdate(l_outcontext, 'VALID_FROM', insis_sys_v10.srv_context.date_format, l_begin_date);
                insis_sys_v10.srv_events.sysevent('INS_POLICY_AGENTS', l_outcontext, l_outcontext, pio_errmsg);
    
                IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
                    putlog(pi_fa_migr_pol_row.control_id, 'INS_POLICY_AGENTS.err[' || l_agent_role_type || ']:' || srv_error.errcollection2string(pio_errmsg));
                    RETURN;
                END IF;
    
--                putlog(pi_fa_migr_pol_row.control_id, '--INS_POLICY_AGENTS.'||l_agent_role_type||'VAT');
                
                insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_AGENT_ID', insis_sys_v10.srv_context.integers_format, NULL);
                --For VAT roles: MARKPS -> MARKPSV; other: <role> ->  <role>VAT
                insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'AGENT_ROLE', l_agent_role_type || CASE WHEN l_agent_role_type = 'MARKPS' THEN 'V' ELSE 'VAT' END);
                insis_sys_v10.srv_events.sysevent('INS_POLICY_AGENTS', l_outcontext, l_outcontext, pio_errmsg);
    
                IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
                    putlog(pi_fa_migr_pol_row.control_id, 'INS_POLICY_AGENTS.err[' || l_agent_role_type || ']:' || srv_error.errcollection2string(pio_errmsg));
                    RETURN;
                END IF;
            END IF;

            l_agent_role_type := l_agent_id_table.NEXT(l_agent_role_type);
                    
        END LOOP;

        
        putlog(pi_fa_migr_pol_row.control_id, '--calcduration:' || l_begin_date || '..' || l_end_date);
----        insis_gen_v10.pol_values.calcduration(l_begin_date, l_end_date, pi_fa_migr_pol_row.insis_product_code, calc_duration, calc_dimension);
----        insis_gen_v10.pol_values.CalcDuration_YMD(l_begin_date, l_end_date, pi_fa_migr_pol_row.insis_product_code, calc_duration, calc_dimension);
----        pol_ps_cons.covobjduration    
--    
        --For special period, duration is set to days
--        IF pi_fa_migr_pol_row.prem_cal_period = 0 THEN 
--            calc_dimension := GVAR_PAS.DUR_DIM_D;
--            calc_duration  := l_end_date - l_begin_date;
--        else
        insis_gen_v10.pol_values.calcduration(l_begin_date, l_end_date, CN_INSR_TYPE, l_calc_duration, l_calc_dimension);
--        end if;

        putlog(pi_fa_migr_pol_row.control_id, 'update policy:' || l_calc_duration || ',' || l_calc_dimension);
        get_sbs_techbr(CN_INSR_TYPE, pi_fa_migr_pol_row.asis_code, l_tech_branch, l_sbs_code);
        
        --todo: usa object type, o %rowtype
        -- ver insis_cust_lpv.intrf_iss003
        -- insis_gen_v10.pol_values.calcduration(v_insr_begin, v_insr_end, i_masterPolicyRow.insr_type, v_calc_duration, v_calc_dimension);
        -- dependent_policy_type := insis_gen_v10.pol_types.get_policy( v_dependent_policy_id ); 
        --...
        BEGIN
            UPDATE insis_gen_v10.POLICY
            SET
                policy_no = pi_fa_migr_pol_row.policy_no,
                policy_name = pi_fa_migr_pol_row.policy_no,
                insr_begin = l_begin_date,
                insr_end = l_end_date,
                date_given = l_begin_date,
                conclusion_date = l_begin_date,
--                date_covered = nvl(l_date_covered,date_covered),
                insr_duration = l_calc_duration,
                dur_dimension = l_calc_dimension,
                payment_duration = l_calc_duration,
                payment_dur_dim = l_calc_dimension,
                attr1 = l_tech_branch,
                attr2 = l_sbs_code,
                attr3 = pi_fa_migr_pol_row.sales_channel_id,
                attr4 = l_office_id,
                attr5 = CASE WHEN pi_fa_migr_pol_row.pay_frequency_desc = 'MENSUAL' THEN 1 --estaba seteado  12                             
                             WHEN pi_fa_migr_pol_row.pay_frequency_desc = 'BIMENSUAL' THEN 2
                             WHEN pi_fa_migr_pol_row.pay_frequency_desc = 'TRIMESTRAL' THEN 3
                             WHEN pi_fa_migr_pol_row.pay_frequency_desc = 'CUATRIMESTRAL' THEN 4
                             WHEN pi_fa_migr_pol_row.pay_frequency_desc = 'SEMESTRAL' THEN 6
                             WHEN pi_fa_migr_pol_row.pay_frequency_desc = 'ANUAL' THEN 12 --estaba seteado  1
                             WHEN pi_fa_migr_pol_row.pay_frequency_desc LIKE 'ESPECIAL%' THEN 0 --PRIMA UNICA
                             ELSE NULL
                        END,  
                payment_type = CASE WHEN pi_fa_migr_pol_row.billing_party_desc = 'REGULAR' THEN 'R' ELSE 'S' END, --single premium
                username = CN_POLICY_USER
            WHERE
                policy_id = l_master_policy_id;
        EXCEPTION
            WHEN OTHERS THEN
                putlog(pi_fa_migr_pol_row.control_id, 'Update_policy.err:'||SQLERRM); 
                srv_error_set('update_policy', NULL, SQLERRM, pio_errmsg);
                RETURN;
        END;
    
    
        --================================================================================================
        -- Updating policy_engagement_billing
        --================================================================================================        
        
        --fill policyengagementbilling structure
        insis_gen_v10.srv_engagement_ds.get_policyengbillingbypolicy(l_outcontext, l_outcontext, pio_errmsg);
        IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
            putlog(pi_fa_migr_pol_row.control_id, 'get_policyengbillingbypolicy.err:' || srv_error.errcollection2string(pio_errmsg));
            RETURN;
        END IF;
        
----        putlogcontext(pi_fa_migr_pol_row.control_id, l_outcontext);
----        putlog(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, 
----               'eng_bill_id:'||insis_gen_v10.srv_policy_data.gengagementbillingrecord.engagement_id || '/' || insis_gen_v10.srv_policy_data.gengagementbillingrecord.num_instalments_period);

        insis_gen_v10.srv_policy_data.gengagementbillingrecord.num_instalments_period := insis_gen_v10.gvar_pas.instalments_period_policy;
        insis_gen_v10.srv_policy_data.gengagementbillingrecord.attr1 := CASE WHEN pi_fa_migr_pol_row.billing_type_desc ='INDIVIDUAL' THEN insis_cust.gvar_cust.BLC_BILL_TYPE_CL_IND  
                                                                             WHEN pi_fa_migr_pol_row.billing_type_desc ='GRUPAL' THEN insis_cust.gvar_cust.BLC_BILL_TYPE_CL_GROUP 
                                                                             ELSE NULL END;
        
        --update 
        l_result    := insis_gen_v10.srv_policy_data.gengagementbillingrecord.updatepengagementbilling(pio_errmsg);
        IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
            putlog(pi_fa_migr_pol_row.control_id, 'updatepengagementbilling.err:' || srv_error.errcollection2string(pio_errmsg));
            RETURN;
        END IF;
        
        --todo : usar objetos genericos
        FOR c_part_rec IN (SELECT * FROM insis_gen_v10.policy_participants WHERE policy_id = l_master_policy_id AND particpant_role IN (insis_cust.gvar_cust.PART_ROL_PAYOR))
        LOOP
       
            insis_gen_v10.srv_prm_policy.spolicyparticipantid(l_outcontext, c_part_rec.participant_id);
            insis_sys_v10.srv_events.sysevent('DEL_POLICY_PARTICIPANS', l_outcontext, l_outcontext, pio_errmsg);
            
            IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
                putlog(pi_fa_migr_pol_row.control_id, 'DEL_POLICY_PARTICIPANS.PAYOR.err:' || srv_error.errcollection2string(pio_errmsg));
                exit;
            END IF;
        END LOOP;
        
        IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
            RETURN;
        END IF;

        --todo: usar loop                
        Record_Participant(cn_proc,
                          l_master_policy_id,
                          0, 
                          insis_cust.gvar_cust.PART_ROL_PAYOR,  
                          pi_fa_migr_pol_row.att_payor_manid,
                          l_begin_date,
                          l_end_date,
                          100,
--                          null,
--                          0,
                          po_outcontext,
                          pio_errmsg);
  
        IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
            putlog(pi_fa_migr_pol_row.control_id, 'Record_Participant.PAYOR.err:' || srv_error.errcollection2string(pio_errmsg));
            RETURN;
        END IF;

        Record_Participant(cn_proc,
                          l_master_policy_id,
                          0, 
                          insis_cust.gvar_cust.PART_ROL_FINBANKBEN,  
                          pi_fa_migr_pol_row.att_financial_ent_manid,
                          l_begin_date,
                          l_end_date,
                          100,
--                          null,
--                          0,
                          po_outcontext,
                          pio_errmsg);
  
        IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
            putlog(pi_fa_migr_pol_row.control_id, 'Record_Participant.FIN.err:' || srv_error.errcollection2string(pio_errmsg));
            RETURN;
        END IF;

        Record_Participant(cn_proc,
                          l_master_policy_id,
                          0, 
                          insis_cust.gvar_cust.PART_ROL_ADDBENPROV,  
                          pi_fa_migr_pol_row.att_benef_prov_manid,
                          l_begin_date,
                          l_end_date,
                          100,
--                          null,
--                          0,
                          po_outcontext,
                          pio_errmsg);
  
        IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
            putlog(pi_fa_migr_pol_row.control_id, 'Record_Participant.BENPROV.err:' || srv_error.errcollection2string(pio_errmsg));
            RETURN;
        END IF;


        putlog(pi_fa_migr_pol_row.control_id, 'starting cover loop' );
        
        l_curr_plan_id := 0;

        OPEN c_fa_migr_cov_plans(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.policy_no);
        --Sample values for one policy:
            --      plan_name , subplan_name
            -----------------------------------
            --      Plan I - Consumo, Titular
            --      Plan I - Consumo, Adicional
            --      Plan II - Convenios, Titular
            --      Plan III - Convenios, Titular
            --      Plan III - Convenios, Adicional
        LOOP
            FETCH c_fa_migr_cov_plans INTO l_fa_migr_cov_plans;
            EXIT WHEN c_fa_migr_cov_plans%NOTFOUND;

            l_curr_plan_id      := l_curr_plan_id + 1;
            l_group_ins_obj_id  := NULL;

            BEGIN
--                    putlog(pi_fa_migr_pol_row.control_id, 'OPEN c_object_types:'||CN_INSR_TYPE ||','|| pi_fa_migr_pol_row.asis_code);

                OPEN c_object_types(CN_INSR_TYPE, pi_fa_migr_pol_row.asis_code);
                --Object name samples:
                    -- Original -> Returned                        
                    --      705    Desgravamen             -> Desgravamen
                    --      706    Desgravamen Main        -> Desgravamen Principal
                    --      707 Desgravamen Additional  -> Desgravamen Adicional
                LOOP
                    l_object_types_rec := NULL;
                    FETCH c_object_types INTO l_object_types_rec;
                    EXIT WHEN c_object_types%NOTFOUND;
                    
                    putlog(pi_fa_migr_pol_row.control_id, 'fa_subplan,obj_name:'|| l_fa_migr_cov_plans.subplan_name || ','|| l_object_types_rec.NAME);
                    
                    --xor(lower(l_fa_migr_cov_plans.subplan_name) like '%titul%', l_object_types_rec.name not like 'Adicional')
                    IF (lower(l_fa_migr_cov_plans.subplan_name) LIKE '%titul%'  AND rtrim(l_object_types_rec.NAME) LIKE '%Adicional%') OR 
                       (lower(l_fa_migr_cov_plans.subplan_name) NOT LIKE '%titul%'  AND rtrim(l_object_types_rec.NAME) NOT LIKE '%Adicional%') THEN
                        putlog(pi_fa_migr_pol_row.control_id, 'validating_subplan, object_type.continue next');
                        CONTINUE; 
                    END IF;

                    IF l_object_types_rec.is_group = 1 THEN
                        l_plan_desc := l_fa_migr_cov_plans.plan_name; 
                    ELSE
                        l_plan_desc := l_fa_migr_cov_plans.plan_name || '-' || l_fa_migr_cov_plans.subplan_name;
                    END IF;
                    
                    putlog(pi_fa_migr_pol_row.control_id,'--INS_GROUP_INS ['||l_curr_plan_id||','||l_plan_desc||'] - ' ||l_object_types_rec.object_type);
        
                    --set to null to get a new value each time
                    insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'OBJECT_ID', insis_sys_v10.srv_context.integers_format, NULL);
                    insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'OBJECT_TYPE', insis_sys_v10.srv_context.integers_format, l_object_types_rec.object_type);
                    insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'MAIN_OBJECT_ID', insis_sys_v10.srv_context.integers_format, NULL);
                    --attributes only for insured objects. for group it is left empty
                    insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'OGPP1', CASE WHEN l_object_types_rec.is_group = 1 THEN NULL ELSE l_fa_migr_cov_plans.plan_min_age END); 
                    insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'OGPP2', CASE WHEN l_object_types_rec.is_group = 1 THEN NULL ELSE l_fa_migr_cov_plans.plan_max_age END); 
                    insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'OGPP3', CASE WHEN l_object_types_rec.is_group = 1 THEN NULL ELSE l_fa_migr_cov_plans.min_outstand END); 
                    insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'OGPP4', CASE WHEN l_object_types_rec.is_group = 1 THEN NULL ELSE l_fa_migr_cov_plans.max_outstand END); 
                    insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'OGPP5', CASE WHEN l_object_types_rec.is_group = 1 THEN NULL ELSE l_fa_migr_cov_plans.min_loan_dur END); 
                    insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'OGPP6', CASE WHEN l_object_types_rec.is_group = 1 THEN NULL ELSE l_fa_migr_cov_plans.max_loan_dur END); 

                    insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'DESCRIPTION', l_plan_desc);

                    --putlog(pi_fa_migr_pol_row.control_id,pi_fa_migr_pol_row.stag_id,'INS_GROUP_INS.pre' );
                    --putlogcontext(pi_fa_migr_pol_row.control_id, l_outcontext);

        
                    insis_sys_v10.srv_events.sysevent('INS_GROUP_INS', l_outcontext, l_outcontext, pio_errmsg);
    
    --                putlogcontext(pi_fa_migr_pol_row.control_id, l_outcontext);
                    IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
                        putlog(pi_fa_migr_pol_row.control_id,'INS_GROUP_INS.err:'||srv_error.ErrCollection2String(pio_errmsg)); 
                        exit;
                    END IF;                                       
        
                    
                    l_object_id := insis_gen_v10.srv_object_data.gogroupinsrecord.object_id;
    
                    --todo:log temporal
--                        putlog(pi_fa_migr_pol_row.control_id,'INS_GROUP_INS.post' );
--                        putlog(pi_fa_migr_pol_row.control_id,'group_id:' ||l_object_id);
                    
    
                    ---                
                    putlog(pi_fa_migr_pol_row.control_id,'--INSERT_INSURED_OBJECT' );
                    insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_ID', insis_sys_v10.srv_context.integers_format, l_master_policy_id);
                    insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ANNEX_ID', insis_sys_v10.srv_context.integers_format, insis_gen_v10.gvar_pas.def_annx_id);
                    insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'INSR_TYPE', insis_sys_v10.srv_context.integers_format, CN_INSR_TYPE);
                    insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'OBJECT_ID', insis_sys_v10.srv_context.integers_format, l_object_id);
                    --sGroupId: 
                    --      null for the main group, 
                    --      main group id for the child groups
                    insis_gen_v10.srv_prm_policy.sGroupId ( l_outcontext, l_group_ins_obj_id);
--                        insis_gen_v10.srv_prm_policy.sSubGroupId ( l_outcontext, 777777 );
                    
                    insis_sys_v10.srv_events.sysevent('INSERT_INSURED_OBJECT', l_outcontext, l_outcontext, pio_errmsg);
        
                    IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
                        putlog(pi_fa_migr_pol_row.control_id,'INSERT_INSURED_OBJECT.err:'||srv_error.ErrCollection2String(pio_errmsg)); 
                        exit;
                    END IF;
        
                    --se extrae solo para informacion
                    --insis_sys_v10.srv_context.getcontextattrnumber(l_outcontext, 'INSURED_OBJ_ID', l_parent_ins_obj_id);
                    --putlog(pi_fa_migr_pol_row.control_id,pi_fa_migr_pol_row.stag_id,'l_parent_ins_obj_id: '||l_parent_ins_obj_id);

                    --TODO: confirmar si se actualiza la tasa de cambio...o solo moneda
                    --TODO: optimizar obteniendo insured_obj_id de contexto, y solo actualizar moneda si es distinta
                    BEGIN
                        UPDATE insis_gen_v10.insured_object io
                        SET io.av_currency = pi_fa_migr_pol_row.currency,
                            io.iv_currency = pi_fa_migr_pol_row.currency
                        WHERE io.policy_id = l_master_policy_id
                          AND io.object_id = l_object_id
                        RETURNING io.insured_obj_id INTO l_ins_obj_id;

                    EXCEPTION
                        WHEN OTHERS THEN
                            putlog(pi_fa_migr_pol_row.control_id, 'INSERT_INSURED_OBJECT.err:'||SQLERRM); 
                            srv_error_set('update_insured_object', NULL, SQLERRM, pio_errmsg);
                            exit;
                    END;
                    putlog(pi_fa_migr_pol_row.control_id,'l_ins_obj_id: '||l_ins_obj_id);
                    
                    --the first insured object id is recorded to assign as group id to the next object
                    IF l_group_ins_obj_id IS NULL THEN
                        l_group_ins_obj_id := l_ins_obj_id;
                    END IF;
                    
                    IF NOT (l_object_types_rec.is_group = 1) THEN

                        ins_pol_special_comm('Obj', l_fa_migr_cov_plans.plan_name, l_fa_migr_cov_plans.subplan_name, pio_errmsg); --agregar el plan 
                        
                    END IF;

                    IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
                        EXIT;
                    END IF;
                    
                    ---
                    --- Selecting specific covers is disabled by now, to load all product's covers
                    ---
                    IF NOT (l_object_types_rec.is_group = 1) THEN
                        putlog(pi_fa_migr_pol_row.control_id,'--FILL_COVERS_FOR_SELECT' );
                        
                        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'INSURED_OBJ_ID', insis_sys_v10.srv_context.integers_format, l_ins_obj_id);
                        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_ID', insis_sys_v10.srv_context.integers_format, l_master_policy_id);
                        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ANNEX_ID', insis_sys_v10.srv_context.integers_format, insis_gen_v10.gvar_pas.def_annx_id);
                        
                        insis_sys_v10.srv_events.sysevent('FILL_COVERS_FOR_SELECT', l_outcontext, l_outcontext, pio_errmsg);
                        insis_sys_v10.srv_context.getcontextattrchar(l_outcontext, 'PROCEDURE_RESULT', l_procedure_result);
                        IF upper(l_procedure_result) = 'FALSE' THEN
                            putlog(pi_fa_migr_pol_row.control_id, 'FILL_COVERS_FOR_SELECT.err:'||srv_error.ErrCollection2String(pio_errmsg)); 
                            srv_error_set('fill_covers_for_select', NULL, 'event_return_false', pio_errmsg);
                            EXIT;
                        END IF;

                        ---
                        putlog(pi_fa_migr_pol_row.control_id,'--SELECTING_ALL_COVERS' );
                        
                        --todo : usar rutina generica. recbir valores, y nombre cobertura: if valores no nulos update ... where cover_type = ...
                        --todo:usar bulk. cargar coberturas al obtener plan
                        UPDATE insis_gen_v10.gen_covers_select gcs
                        SET
                            apply_cover = 1
                        WHERE
                            gcs.policy_id = l_master_policy_id
                        AND gcs.cover_type IN (SELECT cover_type FROM fa_migr_dsgr_mp_cov fa_cov 
                                               WHERE fa_cov.control_id  = pi_fa_migr_pol_row.control_id
                                                 AND fa_cov.policy_no   = l_fa_migr_cov_plans.policy_no
                                                 AND fa_cov.plan_name   = l_fa_migr_cov_plans.plan_name
                                                 AND fa_cov.subplan_name= l_fa_migr_cov_plans.subplan_name
                                                )
                        ; 
                        ---
                        putlog(pi_fa_migr_pol_row.control_id,'--ATTACH_SELECTED_COVERS' );
                        
                        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'INSURED_OBJ_ID', insis_sys_v10.srv_context.integers_format, l_ins_obj_id);
                        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_ID', insis_sys_v10.srv_context.integers_format, l_master_policy_id);
                        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ANNEX_ID', insis_sys_v10.srv_context.integers_format, insis_gen_v10.gvar_pas.def_annx_id);
                            
                        insis_sys_v10.srv_events.sysevent('ATTACH_SELECTED_COVERS', l_outcontext, l_outcontext, pio_errmsg);
                        insis_sys_v10.srv_context.getcontextattrchar(l_outcontext, 'PROCEDURE_RESULT', l_procedure_result);
                            
                        IF upper(l_procedure_result) = 'FALSE' THEN
                            putlog(pi_fa_migr_pol_row.control_id, 'ATTACH_SELECTED_COVERS.err:'||srv_error.ErrCollection2String(pio_errmsg)); 
                            srv_error_set('attach_selected_covers', NULL, 'event_return_false', pio_errmsg);
                            EXIT;
                        END IF;

                        BEGIN
                            putlog(pi_fa_migr_pol_row.control_id, 'UPDATE tariff_percent: ' || l_ins_obj_id || '|' || l_fa_migr_cov_plans.control_id || '|' || l_fa_migr_cov_plans.policy_no || '|' || l_fa_migr_cov_plans.plan_name || '|' ||  l_fa_migr_cov_plans.subplan_name);

--                          UPDATE insis_gen_v10.gen_risk_covered grc
--                            SET 
----                                currency       = pi_fa_migr_pol_row.currency,
--                                insured_value  = nvl((SELECT fa_cov.max_iv   
--                                                        FROM fa_migr_dsgr_mp_cov fa_cov 
--                                                       WHERE fa_cov.control_id   = l_fa_migr_cov_plans.control_id
--                                                         AND fa_cov.policy_no    = l_fa_migr_cov_plans.policy_no
--                                                         AND fa_cov.plan_name    = l_fa_migr_cov_plans.plan_name
--                                                         AND fa_cov.subplan_name = l_fa_migr_cov_plans.subplan_name
--                                                         AND fa_cov.cover_type   = grc.cover_type), 
--                                                     grc.insured_value),
--                                tariff_percent = nvl((SELECT (CASE WHEN UPPER(fa_cov.manual_prem_dim_desc) = 'A' THEN fa_cov.prem_value ELSE  fa_cov.prem_rate END) prem_val
--                                                        FROM fa_migr_dsgr_mp_cov fa_cov 
--                                                       WHERE fa_cov.control_id   = l_fa_migr_cov_plans.control_id
--                                                         AND fa_cov.policy_no    = l_fa_migr_cov_plans.policy_no
--                                                         AND fa_cov.plan_name    = l_fa_migr_cov_plans.plan_name
--                                                         AND fa_cov.subplan_name = l_fa_migr_cov_plans.subplan_name
--                                                         and fa_cov.cover_type   = grc.cover_type
--                                                         ), grc.tariff_percent),
--                                --load value dimension only when tariff percent is present
--                                manual_prem_dimension = 
--                                                    nvl((select case when upper(fa_cov.manual_prem_dim_desc) = 'P' then gvar_pas.prem_dim_p 
--                                                                     when upper(fa_cov.manual_prem_dim_desc) = 'W' then gvar_pas.prem_dim_w
--                                                                     when upper(fa_cov.manual_prem_dim_desc) = 'M' then gvar_pas.prem_dim_m
--                                                                     when upper(fa_cov.manual_prem_dim_desc) = 'V' then gvar_pas.prem_dim_v 
--                                                                     when upper(fa_cov.manual_prem_dim_desc) = 'T' then gvar_pas.prem_dim_t
--                                                                     when upper(fa_cov.manual_prem_dim_desc) = 'I' then gvar_pas.prem_dim_i
--                                                                     when upper(fa_cov.manual_prem_dim_desc) = 'F' then gvar_pas.prem_dim_f
--                                                                     when upper(fa_cov.manual_prem_dim_desc) = 'D' then gvar_pas.prem_dim_d
--                                                                     when upper(fa_cov.manual_prem_dim_desc) = 'A' then gvar_pas.prem_dim_a                                                                   
--                                                            END prem_dim                                                                                                                                                                              
--                                                        FROM fa_migr_dsgr_mp_cov fa_cov 
--                                                       WHERE fa_cov.control_id   = l_fa_migr_cov_plans.control_id
--                                                         AND fa_cov.plan_name    = l_fa_migr_cov_plans.plan_name
--                                                         AND fa_cov.subplan_name = l_fa_migr_cov_plans.subplan_name
--                                                         AND fa_cov.policy_no    = l_fa_migr_cov_plans.policy_no
--                                                         AND fa_cov.cover_type   = grc.cover_type
--                                                         AND nvl(fa_cov.prem_rate,0) > 0 ), 
--                                                     grc.manual_prem_dimension)
--                            WHERE
--                                    insured_obj_id = l_ins_obj_id ;
                                    
                              merge into insis_gen_v10.gen_risk_covered grc
                              using fa_migr_dsgr_mp_cov fa_cov
                              on (fa_cov.cover_type         = grc.cover_type and  grc.insured_obj_id = l_ins_obj_id 
                                 and fa_cov.control_id      = l_fa_migr_cov_plans.control_id
                                 and fa_cov.plan_name       = l_fa_migr_cov_plans.plan_name
                                 and fa_cov.subplan_name    = l_fa_migr_cov_plans.subplan_name
                                 and fa_cov.policy_no       = l_fa_migr_cov_plans.policy_no)
                              WHEN MATCHED THEN
                              update set 
                              grc.insured_value             = nvl(fa_cov.max_iv,grc.insured_value),
                              grc.tariff_percent            = nvl((case when upper(fa_cov.manual_prem_dim_desc) = 'A' then fa_cov.prem_value else  fa_cov.prem_rate end),grc.tariff_percent),
                              grc.manual_prem_dimension     =  nvl((case when upper(fa_cov.manual_prem_dim_desc) = 'P' then gvar_pas.prem_dim_p 
                                                                     when upper(fa_cov.manual_prem_dim_desc) = 'W' then gvar_pas.prem_dim_w
                                                                     when upper(fa_cov.manual_prem_dim_desc) = 'M' then gvar_pas.prem_dim_m
                                                                     when upper(fa_cov.manual_prem_dim_desc) = 'V' then gvar_pas.prem_dim_v 
                                                                     when upper(fa_cov.manual_prem_dim_desc) = 'T' then gvar_pas.prem_dim_t
                                                                     when upper(fa_cov.manual_prem_dim_desc) = 'I' then gvar_pas.prem_dim_i
                                                                     when upper(fa_cov.manual_prem_dim_desc) = 'F' then gvar_pas.prem_dim_f
                                                                     when upper(fa_cov.manual_prem_dim_desc) = 'D' then gvar_pas.prem_dim_d
                                                                     when upper(fa_cov.manual_prem_dim_desc) = 'A' then gvar_pas.prem_dim_a 
                                                                  end), grc.manual_prem_dimension)  ;                      
                                    
                        EXCEPTION
                            WHEN OTHERS THEN
                                putlog(pi_fa_migr_pol_row.control_id, 'UPDATE tariff_percent.err:'||SQLERRM); 
                                srv_error_set('update_covers', NULL, SQLERRM, pio_errmsg);
                                EXIT;
                        END;
                    end if;
                    
                    UPDATE insis_gen_v10.gen_risk_covered grc
                            set 
                                currency  = pi_fa_migr_pol_row.currency
                    WHERE grc.policy_id = l_master_policy_id ;

                end loop; --c_object_types
                
                CLOSE c_object_types;
                            
            EXCEPTION
                WHEN OTHERS THEN
                    IF c_object_types%isopen THEN
                        CLOSE c_object_types;
                    END IF;
                    
                    srv_error_set('c_object_types', NULL, SQLERRM, pio_errmsg);
                    putlog(pi_fa_migr_pol_row.control_id, 'c_object_types.err:'||SQLERRM); 
            END;

            IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
                putlog(pi_fa_migr_pol_row.control_id,'process_cov.err-exiting');
                EXIT;
            END IF;
            
        END LOOP; --c_fa_migr_cov_plans
        CLOSE c_fa_migr_cov_plans;
        
        --load cover commissions
        ins_pol_special_comm('Cob', NULL,NULL, pio_errmsg); 

        --exit routine when error found in loops
        IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN    
            putlog(pi_fa_migr_pol_row.control_id,'process_row.err-exiting');
            RETURN;
        END IF;
         
              
            
        putlog(pi_fa_migr_pol_row.control_id, '--FILL_POLICY_CONDITIONS');
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_ID', insis_sys_v10.srv_context.integers_format, l_master_policy_id);
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ANNEX_ID', insis_sys_v10.srv_context.integers_format, insis_gen_v10.gvar_pas.def_annx_id);
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'INSR_TYPE', insis_sys_v10.srv_context.integers_format, CN_INSR_TYPE);
        insis_sys_v10.srv_events.sysevent('FILL_POLICY_CONDITIONS', l_outcontext, l_outcontext, pio_errmsg);
        insis_sys_v10.srv_context.getcontextattrchar(l_outcontext, 'PROCEDURE_RESULT', l_procedure_result);
        IF upper(l_procedure_result) = 'FALSE' THEN
            putlog(pi_fa_migr_pol_row.control_id, 'FILL_POLICY_CONDITIONS.err:' || srv_error.errcollection2string(pio_errmsg));
            srv_error_set('fill_policy_conditions', NULL, 'event_return_false', pio_errmsg);
            RETURN;
        END IF;

        --
        -- policy_condition updates
        -- dim, val

--        putlog(pi_fa_migr_pol_row.control_id, '--UPDATING POLICY_CONDITION:AS_IS');
        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, 'AS_IS_DESGR', pi_fa_migr_pol_row.asis_code, NULL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, 'PRIMA_MINIMA', NULL, pi_fa_migr_pol_row.minimum_prem, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
--todo: convertir valor
        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id,'TIPO_SUMA_ASEGURADA', CASE WHEN lower(pi_fa_migr_pol_row.iv_type_desc) LIKE '%inicial%' THEN '1' 
                                                                                                                WHEN lower(pi_fa_migr_pol_row.iv_type_desc) LIKE '%insoluto%' THEN '2'
                                                                                                            ELSE 
                                                                                                                NULL 
                                                                                                            END, 
                                                                                                            NULL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, 'ISSUANSE_EXPENSE', NULL, pi_fa_migr_pol_row.iss_expense_perc, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
--todo: convertir valor
        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, 'NO_NOMINATIVA', CASE WHEN lower(pi_fa_migr_pol_row.unidentified_io_flag) LIKE '%no nominativa%' THEN 2
                                                                                                           ELSE 1 END, 
                                                                                                        NULL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
--todo: convertir valor        
        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, 'CONSORCIO', 1/*CASE WHEN lower(pi_fa_migr_pol_row.consortium_flag) = 'no' THEN 1
                                                                                                       ELSE 2 END*/, 
                                                                                                  NULL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, 'ANTTERMILL_X%', NULL, pi_fa_migr_pol_row.term_disease_perc, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
--        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, '????????', null, pi_fa_migr_pol_row.consortium_leader, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, 'BROKER_1_COMM', NULL, pi_fa_migr_pol_row.broker_com_perc, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, 'MARKETER_C_COMM', NULL, pi_fa_migr_pol_row.marketer_comm, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, 'X%_COM_MARK_C', NULL, pi_fa_migr_pol_row.marketer_gu_coll_comm, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, 'X%_COM_MARK_AC', NULL, pi_fa_migr_pol_row.marketer_gu_acq_comm, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, 'X%_COM_MARK_PS', NULL, pi_fa_migr_pol_row.marketer_ps_comm, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, 'Y%_EXP_DEDUCT', NULL, pi_fa_migr_pol_row.expense_deduc_prem_perc, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, 'BENEFIT_COST', 1, pi_fa_migr_pol_row.benef_prov_amount, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
--        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, '??????????', null, pi_fa_migr_pol_row.assist_type, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, 'MIN_ENTRY_AGE', NULL, pi_fa_migr_pol_row.uw_min_entry_age, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, 'MAX_ENTRY_AGE', NULL, pi_fa_migr_pol_row.uw_max_entry_age, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, 'AUTOM_INDEMN', NULL, pi_fa_migr_pol_row.auto_indem_max_amount, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, 'LOAN_TYPE', CASE WHEN lower(pi_fa_migr_pol_row.loan_type) LIKE '%hipotecario%' THEN 1 --todo: constants
                                                                                                       WHEN lower(pi_fa_migr_pol_row.loan_type) LIKE '%consumo%' THEN 2
                                                                                                       ELSE NULL
                                                                                                    END, NULL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, 'MAX_SUM_INSURED', NULL, pi_fa_migr_pol_row.main_cov_max_iv, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, 'MIN_SUM_INSURED', NULL, pi_fa_migr_pol_row.main_cov_min_iv, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, 'MAX_PERM_AGE', NULL, pi_fa_migr_pol_row.main_io_max_perm_age, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);

        --Updates insr_type description in policy_names
--        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_ID', insis_sys_v10.srv_context.integers_format, l_master_policy_id);
--        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ANNEX_ID', insis_sys_v10.srv_context.integers_format, insis_gen_v10.gvar_pas.def_annx_id);
        insis_sys_v10.srv_events.sysevent('CUST_COND_UPD', l_outcontext, l_outcontext, pio_errmsg );
        IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
            putlog(pi_fa_migr_pol_row.control_id, 'CUST_COND_UPD.err:' || srv_error.errcollection2string(pio_errmsg));
            RETURN;
        END IF;
        ----

        putlog(pi_fa_migr_pol_row.control_id, '--LOAD_QUEST');
        insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'REFERENCE_TYPE', 'POLICY');
        insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'TO_LOAD', 'Y');
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_ID', insis_sys_v10.srv_context.integers_format, l_master_policy_id);
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ANNEX_ID', insis_sys_v10.srv_context.integers_format, insis_gen_v10.gvar_pas.def_annx_id);
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'PHOLDER_ID', insis_sys_v10.srv_context.integers_format, insis_gen_v10.srv_policy_data.gpolicyrecord.client_id);

        insis_sys_v10.srv_events.sysevent('LOAD_QUEST', l_outcontext, l_outcontext, pio_errmsg);
        insis_sys_v10.srv_context.getcontextattrchar(l_outcontext, 'PROCEDURE_RESULT', l_procedure_result);
        
        IF upper(l_procedure_result) = 'FALSE' THEN
            putlog(pi_fa_migr_pol_row.control_id, 'LOAD_QUEST.err:' || srv_error.errcollection2string(pio_errmsg));
            RETURN;
        END IF;

        update_quest('EPOLD', CASE WHEN pi_fa_migr_pol_row.epolicy_flag = 'Y' THEN
                                        1
                                     ELSE 
                                        2
                                     END, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);

--        IF pi_fa_migr_pol_row.plan = 6 
--        THEN 
--            update_quest('POL', '2009.01', pi_fa_migr_pol_row.FE_NUM_SAL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
--            update_quest('POL', '2009.02', pi_fa_migr_pol_row.FE_MAX_SI, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
--        END IF;

        ----

--        putlog(pi_fa_migr_pol_row.control_id, '--INSERT_ENDORSEMENT');
--        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_ID', insis_sys_v10.srv_context.integers_format, l_master_policy_id);
--
--        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ANNEX_ID', insis_sys_v10.srv_context.integers_format, insis_gen_v10.
--        gvar_pas.def_annx_id);
--
--        insis_sys_v10.srv_events.sysevent('INSERT_ENDORSEMENT', l_outcontext, l_outcontext, pio_errmsg);
--        insis_sys_v10.srv_context.getcontextattrchar(l_outcontext, 'PROCEDURE_RESULT', l_procedure_result);
--        IF upper(l_procedure_result) = 'FALSE' THEN
--            putlog(pi_fa_migr_pol_row.control_id, 'INSERT_ENDORSEMENT.err');
--            return;
--        END IF;
        
        --Delete specific endorsements according flags
        --todo:usar objetos y constantes, o rutina generica    
--        IF pi_fa_migr_pol_row.legal_limit_clause_flag = 'N' THEN
--            DELETE insis_gen_v10.policy_endorsements
--            WHERE
--                policy_id = l_master_policy_id
--            AND endorsement_code in(608); 
--        END IF;
--
--        IF pi_fa_migr_pol_row.no_salary_limit_flag = 'N' THEN
--            DELETE insis_gen_v10.policy_endorsements
--            WHERE
--                policy_id = l_master_policy_id
--            AND endorsement_code in(601); 
--        END IF;
--
--        
--
        ----

        putlog(pi_fa_migr_pol_row.control_id, '--CALC_PREM');
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_ID', insis_sys_v10.srv_context.integers_format, l_master_policy_id);

        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ANNEX_ID', insis_sys_v10.srv_context.integers_format, insis_gen_v10.
        gvar_pas.def_annx_id);

        insis_sys_v10.srv_events.sysevent('CALC_PREM', l_outcontext, l_outcontext, pio_errmsg);
        IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
            putlog(pi_fa_migr_pol_row.control_id, 'CALC_PREM.err|' || srv_error.errcollection2string(pio_errmsg));
            RETURN;
        END IF;

        
        --Update special clauses
        putlog(pi_fa_migr_pol_row.control_id, '--Update special clauses');
        IF pi_fa_migr_pol_row.special_clauses IS NOT NULL THEN
           UPDATE insis_gen_v10.policy_endorsements
            SET text = pi_fa_migr_pol_row.special_clauses
            WHERE  policy_id = l_master_policy_id
             AND endorsement_code = 656;
        END IF;

        putlog(pi_fa_migr_pol_row.control_id, '--APPL_CONF');
        insis_sys_v10.srv_events.sysevent('APPL_CONF', l_outcontext, l_outcontext, pio_errmsg);
        IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
            putlog(pi_fa_migr_pol_row.control_id, 'APPL_CONF.err|' || srv_error.errcollection2string(pio_errmsg));
            RETURN;
        END IF;                        

        ----

        IF pi_fa_migr_pol_row.policy_state_desc = CN_FINAL_STATUS_REGISTERED THEN
            putlog(pi_fa_migr_pol_row.control_id, '--APPL_CONV');
            insis_sys_v10.srv_events.sysevent('APPL_CONV', l_outcontext, l_outcontext, pio_errmsg);
            IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
                putlog(pi_fa_migr_pol_row.control_id, 'APPL_CONV.err|' || srv_error.errcollection2string(pio_errmsg));
                RETURN;
            END IF;

        END IF;
        
        l_staff_id := NULL;
        OPEN c_staff_id;
        FETCH c_staff_id INTO l_staff_id;
        IF c_staff_id%NOTFOUND
        THEN
            NULL;
        END IF;
        CLOSE c_staff_id;
        
        putlog(pi_fa_migr_pol_row.control_id, 'Update policy final:'||l_master_policy_id || ',' || l_staff_id);
        
        UPDATE insis_gen_v10.POLICY
        SET
            policy_name = pi_fa_migr_pol_row.policy_no, --todo : validar si es necesario
--            date_covered = nvl(l_date_covered,date_covered),
            staff_id = l_staff_id
        WHERE
            policy_id = l_master_policy_id;

        putlog(pi_fa_migr_pol_row.control_id, 'process_row|end');
    EXCEPTION
        WHEN OTHERS THEN
            srv_error.setsyserrormsg(l_srverrmsg, 'fa_cust_migr_dsgr_mp.process_row', SQLERRM, SQLCODE);
            srv_error.seterrormsg(l_srverrmsg, pio_errmsg);
            putlog(pi_control_id, cn_proc || '|end_excep|' || SQLERRM);
    END process_row;


--
--------------------------------------------------------------------------------
-- Name: process_job
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
-- Purpose:  Process a block of records on "cn_stat_rec_valid" status
--
-- Input parameters:
--    pi_control_id 
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
    ) IS

        cn_proc    VARCHAR2(100) := 'process_job_' || pi_control_id || '_' || pi_id_init || '..' || pi_id_end;
        v_pio_err  srverr;
        v_errm     VARCHAR(4000);
        gphase     VARCHAR(120);
        gstage     VARCHAR(120) := 'process_job';
        v_file_id  NUMBER;
    BEGIN
        --starting point for log sequence
        --sample: 1200000000000 + 34223 => 120342231000
        --added page X0000 to differentiate from Process_SPF_Data 
        l_log_seq  := l_log_seq_ini + ( pi_control_id * 1000000 ) + ( pi_id_page * 10000 );
        putlog(pi_control_id, cn_proc || '|start|params: ' || pi_control_id || ',' || pi_file_id || ',' || pi_file_name || ',' ||
        pi_id_init || ',' || pi_id_end || ',' || pi_id_page);

        v_file_id  := pi_file_id;
        FOR r_ins_det IN (
            SELECT
                *
            FROM
                cust_migration.fa_migr_dsgr_mp_pol stg
            WHERE
                    stg.control_id = pi_control_id
                AND stg.stag_id BETWEEN pi_id_init AND pi_id_end
                AND stg.att_status_row = CN_STAT_REC_VALID
            ORDER BY
                control_id,
                stag_id
        ) LOOP
            SAVEPOINT generate_job_sp;
            
            v_pio_err := NULL;

            process_row(pi_control_id, r_ins_det, v_pio_err);
            
            IF srv_error.rqstatus(v_pio_err) THEN
                COMMIT;
            ELSE
--                ROLLBACK TO generate_job_sp;
                COMMIT; --todo: por ahora deja poliza para revisar error
                v_errm := srv_error.errcollection2string(v_pio_err);

                ins_error_stg(r_ins_det.control_id, r_ins_det.stag_id, 'ERR', 0, v_errm, v_pio_err);

                UPDATE cust_migration.fa_migr_dsgr_mp_pol stg
                SET
                    stg.att_status_row = CN_STAT_REC_ERROR
                WHERE
                        stg.control_id = r_ins_det.control_id
                    AND stg.stag_id = r_ins_det.stag_id;

                COMMIT;

                putlog(pi_control_id, cn_proc || '|ERROR_AND_ROLLBACK : ' || v_errm);
                insis_cust_lpv.sys_schema_utils.log_poller_error_process(v_file_id, pi_file_name, CN_POLLER_CODE, v_errm, gstage);
                COMMIT;
            END IF;

        END LOOP;

        COMMIT;
        putlog(pi_control_id, cn_proc || '|end');
    END process_job;

--
--------------------------------------------------------------------------------
-- Name: .generate_jobs
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
-- Purpose: generate jobs to process set of records
--
-- Input parameters:
--    pi_control_id 
--    file_id 
--    file_name 
--
-- Output parameters:
--
--------------------------------------------------------------------------------

    FUNCTION generate_jobs (
        pi_control_id  IN  NUMBER,
        pi_file_id     IN  NUMBER,
        pi_file_name   IN  VARCHAR
    ) RETURN BOOLEAN IS

        l_chunk_sql         VARCHAR2(1000);
        l_sql_stmt          VARCHAR2(1000);
        l_job_name          VARCHAR2(1000);
        l_task_name         VARCHAR(1000);
        l_try               NUMBER;
        l_status            NUMBER;
        l_no_records        NUMBER;
        v_file_id           NUMBER;
        l_job_id            PLS_INTEGER;
    --
        l_number_of_jobs    NUMBER := cn_jobs_number;
        l_job_size          NUMBER;
        l_number_of_errors  NUMBER;
        l_jobs_running      NUMBER;
        l_job_id_array      VARCHAR(4000);
        v_n_job             NUMBER := 0;
        v_jobdef            sys.job_definition;
        v_jobdef_arr        sys.job_definition_array;
        v_ret               BOOLEAN := FALSE;
        CURSOR c_job_size (
            pi_num_jobs IN NUMBER
        ) IS
        SELECT
            round(COUNT(*) / pi_num_jobs) AS jobs_size
        FROM
            cust_migration.fa_migr_dsgr_mp_pol
        WHERE
            control_id = pi_control_id;

        --to-do: validate against cust_migration.fa_migr_poller_err where err_type = 'error'

        CURSOR c_errors_exist (
            pi_ctrol_id IN NUMBER
        ) IS
        SELECT
            COUNT(*) AS no_of_errors
        FROM
            cust_migration.fa_migr_poller_err
        WHERE
                control_id = c_errors_exist.pi_ctrol_id
            AND err_type = cn_stat_rec_error;

    BEGIN
        putlog(pi_control_id, 'generate_jobs|start|params: ' || pi_control_id || ',' || pi_file_id || ',' || pi_file_name);

        OPEN c_job_size(l_number_of_jobs);
        FETCH c_job_size INTO l_job_size;
        CLOSE c_job_size;
    --
        v_file_id     := pi_file_id;
        IF l_job_size = 0 THEN
            l_job_size := 1;
        END IF;
        v_jobdef_arr  := sys.job_definition_array();
        l_task_name   := 'fa_cust_migr_spf-' || pi_control_id || '-';
        FOR c_st IN (
            WITH A AS (
                SELECT
                    stag_id,
                    ROW_NUMBER() OVER(PARTITION BY control_id
                        ORDER BY
                            stag_id
                    ) AS rownum_
                FROM
                    cust_migration.fa_migr_dsgr_mp_pol
                WHERE
                        control_id = pi_control_id
                    AND att_status_row = cn_stat_rec_valid
            ), b AS (
                SELECT
                    stag_id,
                    trunc(
                        CASE
                            WHEN mod(rownum_, l_job_size) = 0 THEN
                                rownum_ / l_job_size
                            ELSE
                                rownum_ / l_job_size + 1
                        END
                    ) AS page_num
                FROM
                    A
            ), c AS (
                SELECT
                    MIN(stag_id)     AS fv,
                    MAX(stag_id)     AS lv,
                    page_num
                FROM
                    b
                GROUP BY
                    page_num
            )
            SELECT
                fv,
                lv,
                c.page_num
            FROM
                c
            ORDER BY
                c.page_num
        ) LOOP
            l_sql_stmt                := ' begin fa_cust_migr_dsgr_mp.process_job (' || pi_control_id || ', ' || v_file_id || ', ' || chr(39) || pi_file_name ||
            chr(39) || ', ' || c_st.fv || ', ' || c_st.lv || ', ' || c_st.page_num || '); end;';

            v_n_job                   := v_n_job + 1;
            l_job_name                := l_task_name || v_n_job;
            putlog(pi_control_id, 'generate_jobs|' || l_job_name || '|' || l_sql_stmt);
            
            v_jobdef_arr.EXTEND;
            v_jobdef             := sys.job_definition(job_name     => '"' || l_job_name || '"', job_style => 'REGULAR', number_of_arguments => 0,
                                                       job_type     => 'PLSQL_BLOCK', job_action => l_sql_stmt,
                                                       start_date   => sysdate, 
                                                       enabled      => TRUE, 
                                                       auto_drop    => TRUE, 
                                                       comments     => 'fa_cust_migr_spf - SPF'--,
                                            --instance_id    => 0
                   );

            v_jobdef_arr(v_n_job)     := v_jobdef;
        END LOOP;

        putlog(pi_control_id, 'generate_jobs|waiting jobs...');
        dbms_scheduler.create_jobs(v_jobdef_arr, 'TRANSACTIONAL'); --TRANSACTIONAL STOpi_ON_FIRST_ERROR  ABSORB_ERRORS 
        dbms_lock.sleep(2);
        WHILE ( TRUE ) LOOP
            SELECT
                COUNT(1)
            INTO l_jobs_running
            FROM
                all_scheduler_running_jobs
            WHERE
                job_name LIKE '%' || l_task_name || '%';

            IF l_jobs_running > 0 THEN
--            dbms_output.put_line('Esperando= '|| l_jobs_running || '  '|| to_char(sysdate,'DD/MM/YYYY HH24:MI:SS'));
                dbms_lock.sleep(10);
            ELSE
--            dbms_output.put_line('Después de terminar los jobs= '|| l_jobs_running || '  '|| to_char(sysdate,'DD/MM/YYYY HH24:MI:SS') );
                EXIT;
            END IF;
        END LOOP;

        putlog(pi_control_id, 'generate_jobs|checking errors...');

/*
    open  c_errors_exist ( file_id );
    fetch c_errors_exist into l_number_of_errors;
    close c_errors_exist;

    --to-do revisar si quitar
    if l_number_of_errors > 0 then
--        sys_schema_utils.update_poller_process_status (pi_control_id , 'ERROR');
        raise_application_error( -20001, 'generate_job with ID ' || pi_control_id || ' finished with errors.');
    else
--        sys_schema_utils.update_poller_process_status (pi_control_id , 'SUCCESS');
        null;
    end if;
 */
        putlog(pi_control_id, 'generate_jobs|end');
      --      putlog ('generate_jobs|end_error|'||sqlerrm);
        v_ret         := TRUE;
        RETURN v_ret;
    EXCEPTION
        WHEN OTHERS THEN
            putlog(pi_control_id, 'generate_jobs|end_error|' || SQLERRM);
            v_ret := FALSE;
            RETURN v_ret;
    END generate_jobs;

    --
    -- set_report_status
    --

    PROCEDURE set_report_status (
        pi_control_id  cust_migration.fa_migr_poller_err.control_id%TYPE,
        pi_status      cust_migration.fa_migr_poller_err.err_type%TYPE
    ) AS
    BEGIN
        IF pi_status = cn_ready_for_rep THEN
            --creates record for report process
            INSERT INTO cust_migration.fa_migr_poller_err (
                poller_code,
                control_id,
                stag_id,
                err_seq,
                err_type,
                err_code,
                err_mess
            ) VALUES (
                CN_POLLER_CODE,                
                pi_control_id,
                0,
                0,
                pi_status,
                NULL,
                '--Batch ready for reporting--'
            );

        ELSE
            UPDATE cust_migration.fa_migr_poller_err
            SET
                err_type = cn_report_gen,
                err_mess = '--Report generated--'
            WHERE
                    control_id = pi_control_id
                AND stag_id = 0
                AND err_seq = 0
                AND err_type = cn_ready_for_rep;

        END IF;
    END set_report_status;

--
--------------------------------------------------------------------------------
-- Name: process_main
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
-- Purpose: Main poller process. Complete, validate and generate job data
--
-- Input parameters:
--    pi_control_id 
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
    ) IS
--
        v_fa_migr_dsgr_mp_pol  cust_migration.fa_migr_dsgr_mp_pol%ROWTYPE;
        v_code                VARCHAR(4000);
        pio_err               srverr;
        v_errm                VARCHAR(4000);
        v_file_id             NUMBER;
        v_result              BOOLEAN := FALSE;--
    BEGIN
    --starting point for log sequence
    --sample: 1200000000000 + 34223 => 1203422300000
        l_log_seq   := l_log_seq_ini + ( pi_control_id * 1000000 );
        l_log_proc  := pi_control_id;
        putlog(pi_control_id, 'process_main|start|params:' || pi_control_id || ',' || pi_file_id || ',' || pi_file_name);

        v_result    := TRUE;

        -- get file_id (new or current)
        IF pi_file_id IS NULL THEN
            insis_cust_lpv.sys_schema_utils.log_poller_process(pi_control_id, pi_file_name, cn_poller_code, 'Post-Process ', v_file_id);
        ELSE
            v_file_id := pi_file_id;
        END IF;

        UPDATE insis_cust_lpv.sys_poller_process_ctrl
        SET
            file_id = v_file_id
        WHERE
            sys_poller_process_ctrl_id = process_main.pi_control_id;

        COMMIT;
        v_result    := TRUE;

    --
    -- PROCESS
    --

    --
    --1) complete data necesary for validation and process
    --
        v_code      := 'complete_data';
        v_result    := complete_data(pi_control_id, v_file_id, pi_file_name);

    --
    --2) validate data
    --
        IF v_result THEN
            v_code    := 'validate_data';
            v_result  := validate_data(pi_control_id, v_file_id, pi_file_name);
        END IF;

    --
    --3) generate jobs to process data by record sets
    --
        IF v_result THEN
            v_code    := 'generate_jobs';
            v_result  := generate_jobs(pi_control_id, v_file_id, pi_file_name);
        END IF;

    ----------------------------------------
    --
    -- RESULT
    --
        IF v_result THEN
            insis_cust_lpv.sys_schema_utils.update_poller_process_status(pi_control_id, 'SUCCESS');
        ELSE
            insis_cust_lpv.sys_schema_utils.log_poller_error_process(v_file_id, pi_file_name, cn_poller_code, 'Error', v_code);
            insis_cust_lpv.sys_schema_utils.update_poller_process_status(pi_control_id, 'ERROR');
--        raise_application_error( -20001, 'generate_job with ID ' || pi_control_id || ' finished with errors.');
        END IF;

        set_report_status(pi_control_id, cn_ready_for_rep);
        putlog(pi_control_id, 'process_main|end');
    EXCEPTION
        WHEN OTHERS THEN
            insis_cust_lpv.sys_schema_utils.log_poller_error_process(v_file_id, pi_file_name, cn_poller_code, SQLERRM, 'process_main');
            insis_cust_lpv.sys_schema_utils.update_poller_process_status(pi_control_id, 'ERROR');
            putlog(pi_control_id, 'process_main|end_error');
    END process_main;


    ----------------------------------------
    -- Name: reverse_process
    -- Objective: Revert data to reprocess a dataset
    --

    PROCEDURE reverse_process (
        pi_control_id  IN  NUMBER,
        pi_file_id     IN  NUMBER,
        pi_file_name   IN  VARCHAR
    ) IS
--

        v_fa_migr_dsgr_mp_pol  cust_migration.fa_migr_dsgr_mp_pol%ROWTYPE;
        v_code                VARCHAR(4000);
        pio_err               srverr;
        v_errm                VARCHAR(4000);
        v_file_id             NUMBER;
        v_result              BOOLEAN := FALSE;--
    BEGIN
    --starting point for log sequence
    --sample: 1200000000000 + 34223 => 1203422300000
        l_log_seq   := l_log_seq_ini + ( pi_control_id * 1000000 );
        l_log_proc  := pi_control_id;
        DELETE sta_log
        WHERE
                table_name = CN_POLLER_OBJECT
            AND batch_id LIKE to_char(reverse_process.pi_control_id) || '%';

        DELETE cust_migration.fa_migr_poller_err
        WHERE
            control_id = reverse_process.pi_control_id; 

        --se actualizan policy_no generados para que no se dupliquen

        UPDATE insis_gen_v10.POLICY
        SET
            policy_no = substr(policy_id, 1, 4) || substr(policy_id, 7, 6)
--            ,policy_name = substr(policy_id, 1, 4) || substr(policy_id, 7, 6)
        WHERE
            policy_no IN (
                SELECT
                    fa.policy_no
                FROM
                    cust_migration.fa_migr_dsgr_mp_pol fa
                WHERE
                    fa.control_id = reverse_process.pi_control_id
            )
            AND policy_no <> substr(policy_id, 1, 4) || substr(policy_id, 7, 6);

        putlog(pi_control_id, 'reverse_process|start|params:' || pi_control_id || ',' || pi_file_id || ',' || pi_file_name);

        putlog(pi_control_id, 'reverse_process|updating att');
        UPDATE cust_migration.fa_migr_dsgr_mp_pol d
        SET
            att_status_row = CN_STAT_REC_LOAD,
            att_policy_id = NULL,
            att_pholder_manid = NULL,
            att_financial_ent_manid = NULL,
            att_payor_manid = NULL,
            att_internal_agent_id = NULL,
            att_broker_agent_id = NULL,
            att_mark_c_agent_id = NULL,
            att_mark_gu_coll_agent_id = NULL,
            att_mark_gu_acq_agent_id = NULL,
            att_mark_ps_agent_id = NULL,
            att_benef_prov_manid = NULL
        WHERE
            control_id = reverse_process.pi_control_id;

        COMMIT;
        putlog(pi_control_id, 'reverse_process|end');
    EXCEPTION
        WHEN OTHERS THEN
            insis_cust_lpv.sys_schema_utils.log_poller_error_process(v_file_id, pi_file_name, cn_poller_code, SQLERRM, 'Process_SPF_Data');
            insis_cust_lpv.sys_schema_utils.update_poller_process_status(pi_control_id, 'ERROR');
            putlog(pi_control_id, 'reverse_process|end_error');
    END reverse_process;


    --
    -- get_last_report_proc
    --

    PROCEDURE get_last_report_proc (
        po_poller_id     OUT  NUMBER,
        po_file_name     OUT  VARCHAR2,
        po_success_flag  OUT  INTEGER
    ) IS
    BEGIN
        l_log_proc       := '0';
--        putlog('get_last_report_proc|start| ' || po_poller_id);
        po_success_flag  := 1;
        SELECT
            sys_poller_process_ctrl_id,
            substr(file_name, 0, instr(file_name, '.') - 1) || '-' || file_id || '-' || to_char(date_init, 'YYYYMMDD_HH24MISS') || '.xlsx'
        INTO
            po_poller_id,
            po_file_name
        FROM
            insis_cust_lpv.sys_poller_process_ctrl
        WHERE
            sys_poller_process_ctrl_id = (
                SELECT
                    control_id
                FROM
                    (   --recover oldest process pending process that has data processed (status 2 or 3))
                        SELECT
                            control_id
                        FROM
                            cust_migration.fa_migr_poller_err ctrl
                        WHERE
                                stag_id = 0
                            AND err_seq = 0 --first record
                            AND err_type = cn_ready_for_rep --record ready for report
                            AND EXISTS (
                                SELECT
                                    1
                                FROM
                                    cust_migration.fa_migr_dsgr_mp_pol stg
                                WHERE
                                        stg.control_id = ctrl.control_id
                                    AND stg.att_status_row IN (
                                        cn_stat_rec_valid,
                                        cn_stat_rec_error
                                    )
                            )
                        ORDER BY
                            control_id ASC
                    )
                WHERE
                    ROWNUM = 1
            );

        putlog(po_poller_id, 'get_last_report_proc|end| ' || po_poller_id);
    EXCEPTION
        WHEN OTHERS THEN
            po_success_flag := 0;
            putlog(po_poller_id, 'get_last_report_proc|end_err| ' || SQLERRM);
    END get_last_report_proc;

    --
    -- upd_last_report_proc 
    --  

    PROCEDURE upd_last_report_proc (
        pi_control_id_rep   IN  NUMBER, --report process itself
        pi_file_id          IN  NUMBER,
        pi_control_id_proc  IN  NUMBER  --process with data to be reported 
    ) IS
    BEGIN
        l_log_proc := pi_control_id_rep;
        putlog(pi_control_id_rep, 'upd_last_report_proc|start|control_id_rep,control_id_proc: ' || pi_control_id_rep || ',' || pi_control_id_proc);
        UPDATE insis_cust_lpv.sys_poller_process_ctrl
        SET
            file_id = upd_last_report_proc.pi_file_id
        WHERE
            sys_poller_process_ctrl_id = pi_control_id_rep;

        insis_cust_lpv.sys_schema_utils.update_poller_process_status(pi_control_id_rep, 'SUCCESS');
        set_report_status(pi_control_id_proc, cn_report_gen);
        COMMIT;
        putlog(pi_control_id_rep, 'upd_last_report_proc|end');
    EXCEPTION
        WHEN OTHERS THEN
            putlog(pi_control_id_rep, 'upd_last_report_proc|end_err|' || SQLERRM);
    END upd_last_report_proc;

END fa_cust_migr_dsgr_mp;