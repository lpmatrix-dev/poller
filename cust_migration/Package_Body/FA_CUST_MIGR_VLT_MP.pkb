create or replace PACKAGE BODY                fa_cust_migr_vlt_mp AS
    --------------------------------------------------------------------------------
    -- Name: fa_cust_migr_vlt_mp
    -------------------------------------
    -- Purpose: This poller creates **master policies** for VIDA LEY TRABAJADORES product (2009-1)
    --          It is intended to be used during migration process
    -- Type: PACKAGE
    -- Versioning:
    --     LPV-FRAMEND0     2020-03-09      creation
    --     LPV-FRAMEND0     2020-04-05      ISS033-Load endorsements
    --     LPV-FRAMEND0     2020-04-05      ISS034-Complete policy conversion
    --     LPV-FRAMEND0     2020-04-07      ISS035-Include more insured objets
    --     LPV-FRAMEND0     2020-04-07      ISS036-Add BROKER/BROKERVAT agent
    --     LPV-FRAMEND0     2020-03-09      ISS037-Add policy_state
    --     LPV-FRAMEND0     2020-04-13      ISS039-Add report
    --     LPV-FRAMEND0     2020-04-13      ISS041-Missing premium for manual premium
    --     LPV-FRAMEND0     2020-04-25      ISS038-Assign insured object plan
    --     LPV-FRAMEND0     2020-04-05      ISS033-Load endorsements; deleting according flags
    --     LPV-FRAMEND0     2020-04-26      ISS044-Load addtional cover depending on legal cover flag
    --     LPV-FRAMEND0     2020-04-26      ISS045-Agent commission is received as percent, no need to x100 value
    --     LPV-FRAMEND0     2020-04-26      ISS046-Questionarie values only apllies for 6-Tailored Plan
    --     LPV-FRAMEND0     2020-04-26      ISS047-Fix Master Premium Period
    --     LPV-FRAMEND0     2020-04-27      ISS048-Fix endorsements. Add 601, remove 607
    --     LPV-FRAMEND0     2020-04-27      ISS043-Date datatype
    --     LPV-FRAMEND0     2020-04-27      ISS050-Policy and Product codes (Policy_no, policy_name, SBS Code, Technical Branch)
    --     LPV-FRAMEND0     2020-04-27      ISS051-Fix Legal limits in policy conditions
    --     LPV-FRAMEND0     2020-04-30      ISS054-Add Salary limits by covers
    --     LPV-FRAMEND0     2020-04-30      ISS055-Fix Electronic Policy question
    --     LPV-FRAMEND0     2020-04-30      ISS053-Set insured object's currency
    --     LPV-FRAMEND0     2020-05-02      ISS056-Fix value for manual Premium Dimension ()
    --     LPV-FRAMEND0     2020-05-02      ISS052-Select additional cover for Plan 6
    --     LPV-FRAMEND0     2020-05-02      ISS056-Fix value for manual Premium Dimension
    --     LPV-FRAMEND0     2020-09-03      ISS099-Rename objects
    --     LPV-FRAMEND0     2020-09-03      ISS086-Add Internal agent 0 group
    --     LPV-FRAMEND0     2020-09-14      ISS102-Economic group set as optional
    --     LPV-JAVCANC0     2020-11-08      Sprint7 - Delete legal_limit_flag and add validate LOV
    ---------------------------------------------------------------------------------

    l_log_seq_ini           cust_migration.sta_log.rec_count%TYPE := 1400000000000;
    l_log_seq               cust_migration.sta_log.rec_count%TYPE := l_log_seq_ini;
    l_log_proc              cust_migration.sta_log.batch_id%TYPE;
    l_err_seq               cust_migration.fa_migr_vley_err.errseq%TYPE := 0;

    lc_log_table_name       CONSTANT cust_migration.sta_log.table_name%TYPE := 'fa_cust_migr_vlt_mp';
    lc_poller_name          CONSTANT insis_cust_lpv.sys_poller_process_ctrl.poller_name%TYPE := 'XLS_MIGR_VLT_MP';

    lc_stat_rec_load        CONSTANT fa_migr_vlt_mp_pol.att_status_rec%TYPE := '1'; --Loaded in staging table
    lc_stat_rec_valid       CONSTANT fa_migr_vlt_mp_pol.att_status_rec%TYPE := '2'; --Valid for process / sucessful
    lc_stat_rec_error       CONSTANT fa_migr_vlt_mp_pol.att_status_rec%TYPE := '3'; --invalid for process/errors during process

    lc_group_type_def       CONSTANT insis_gen_v10.policy_benefit_groups.group_type%TYPE := 'SEGMENT'; --Group type 'Customer segmentation'
    lc_process_user         CONSTANT VARCHAR2(20) := 'insis_gen_v10';
    lc_policy_user          CONSTANT VARCHAR2(20) := 'CUST_MIGRATION';


    --------------------------------------------------------------------------------
    -- Name: fa_cust_migr_vlt_mp.putlog
    -------------------------------------
    -- Purpose: record information in log
    -- Type: PROCEDURE
    -- Versioning:
    --     LPV-FRAMEND0     2020-03-09      creation
    ---------------------------------------------------------------------------------
    PROCEDURE putlog (
        pi_sys_ctrl_id  IN  NUMBER,
        pi_stg_id       IN  NUMBER,
        pi_msg          IN  VARCHAR
    ) IS
    BEGIN

        sta_utils.log_message(pi_table_name => lc_log_table_name,
                              pi_batch_id   => l_log_proc,
                              pi_counter    => l_log_seq,
                              pi_message    => '['||pi_stg_id||']' || pi_msg);

        dbms_output.put_line('[' || systimestamp || ']; fa_cust_migr_vlt_mp[' || l_log_seq || '] ' || pi_msg);

        l_log_seq := l_log_seq + 1;

    END putlog;

    --------------------------------------------------------------------------------
    -- Name: fa_cust_migr_vlt_mp.putlogcontext
    -------------------------------------
    -- Purpose: record information from context in log
    -- Type: PROCEDURE
    -- Status: ACTIVE
    -- Versioning:
    --     LPV-FRAMEND0     2020-03-09      creation
    ---------------------------------------------------------------------------------
    PROCEDURE putlogcontext (
        p_sys_ctrl_id  IN NUMBER,
        p_context      srvcontext
    ) AS
        v_text VARCHAR2(4000);
    BEGIN
        FOR r IN p_context.first..p_context.last LOOP
            v_text := v_text || r || ']|' || p_context(r).attrcode || '|' || p_context(r).attrtype || '|' || p_context(r).attrformat ||
            '|' || p_context(r).attrvalue;
        END LOOP;

        IF length(v_text) <= 1500 THEN
            putlog(p_sys_ctrl_id, 0, v_text);
        ELSE
            putlog(p_sys_ctrl_id, 0, substr(v_text,1,1500));
            putlog(p_sys_ctrl_id, 0, substr(v_text,1501,3000));
        END IF;
    END putlogcontext;

    --------------------------------------------------------------------------------
    -- Name: fa_cust_migr_vlt_mp.srv_error_set
    -------------------------------------
    -- Purpose: create error in srverr object
    -- Type: PROCEDURE
    -- Status: ACTIVE
    -- Versioning:
    --     LPV-FRAMEND0     2020-04-14      creation
    ---------------------------------------------------------------------------------
    PROCEDURE srv_error_set(
        pi_fn_name     IN varchar2,
        pi_error_code  IN varchar2,
        pi_error_msg   IN varchar2,
        pio_errmsg     IN OUT SrvErr)
    AS
        l_errmsg                   srverrmsg;
    BEGIN
        insis_sys_v10.srv_error.seterrormsg(l_errmsg, pi_fn_name, nvl(pi_error_code, 'SYSERROR'), pi_error_msg);
        insis_sys_v10.srv_error.seterrormsg(l_errmsg, pio_errmsg);
    EXCEPTION
        WHEN OTHERS THEN
            srv_error.setsyserrormsg( l_errmsg, 'srv_error_set', SQLERRM );
            srv_error.seterrormsg( l_errmsg, pio_errmsg );
    END srv_error_set;

    --------------------------------------------------------------------------------
    -- Name: fa_cust_migr_vlt_mp.tdate
    -------------------------------------
    -- Purpose: create error in srverr object
    ---------------------------------------------------------------------------------
    FUNCTION tdate(pi_strdate   varchar2) RETURN date
    AS
        l_date  date;
    BEGIN
        BEGIN
            l_date := to_date(pi_strdate, 'dd/mm/yyyy');

        EXCEPTION
            WHEN others THEN
                l_date := NULL;
        END;

        RETURN l_date;

    END tdate;

    --------------------------------------------------------------------------------
    -- Name: fa_cust_migr_vlt_mp.upload_file_data
    -------------------------------------
    -- Purpose: record information from context in log
    -- Type: PROCEDURE
    -- Status: ACTIVE
    -- Versioning:
    --     LPV-FRAMEND0     2020-03-09      creation
    ---------------------------------------------------------------------------------
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
    ) IS
        pio_errmsg                srverr;
        v_id                      PLS_INTEGER;
    BEGIN
        l_log_proc := pi_control_id;

        --todo: usar indice descendiente o enviar secuencia desde configuracion poller
        SELECT
            COUNT(0) + 1
        INTO v_id
        FROM
            cust_migration.fa_migr_vlt_mp_pol
        WHERE
            control_id = pi_control_id;

        INSERT INTO fa_migr_vlt_mp_pol (
            control_id,
            stag_id,
            rowseq,
            insr_type,
            as_is,
            holder_inx_id,
            policy_state,
            internal_agent_no,
            internal_agent_name,
            econo_group_code,
            econo_group_name,
            master_policy_no,
            master_begin_date,
            master_end_date,
            epolicy_flag,
            coverdate,
            broker_inx_id,
            brok_comm_perc,
            currency,
            channel,
            office,
            frequency,
            consortium_flag,
            tender_flag,
            billing_type,
            prem_cal_period,
            billing_by,
            issuing_min_prem,
            empl1_rate,
            empl2_rate,
            high_risk1_rate,
            high_risk2_rate,
            low_risk1_rate,
            low_risk2_rate,
            natdeath_sal,
            accdeath_sal,
            itpa_sal,
            PLAN,
            legal_cov_flag,
            fe_num_sal,
            fe_max_si,
            desg_num_sal,
            desg_max_si,
            homeless_num_sal,
            homeless_max_si,
            anttermill_num_sal,
            anttermill_max_si,
            cancer_death_num_sal,
            cancer_death_max_si,
            cancer_num_sal,
            cancer_max_si,
            critmyo_num_sal,
            critmyo_max_si,
            cistroke_num_sal,
            cistroke_max_si,
            cicrf_num_sal,
            cicrf_max_si,
            cimultscl_num_sal,
            cimultscl_max_si,
            cicoma_num_sal,
            cicoma_max_si,
            cibypass_num_sal,
            cibypass_max_si,
            critill_num_sal,
            critill_max_si,
            blindness_num_sal,
            blindness_max_si,
            critburn_num_sal,
            critburn_max_si,
            posthum_child_num_sal,
            posthum_child_max_si,
            deafness_num_sal,
            deafness_max_si,
            fam_sal_perc,
            fam_num_sal,
            fam_max_si,
            reprem_num_sal,
            reprem_max_si,
            inabwork_num_sal,
            inabwork__max_si,
            transfer_num_sal,
            transfer_max_si,
            unid_policy_flag,
--            legal_limit_flag,
            legal_limit_clause_flag,
            no_salary_limit_flag,
            indem_pay_clause_flag,
            claim_pay_clause_flag,
            currency_clause_flag,
            waiting_clause_flag,
            special_clause_text
        ) VALUES (
            pi_control_id,
            v_id,
            pi_rowseq,
            pi_insr_type,
            pi_as_is,
            pi_holder_inx_id,
            pi_policy_state,
            pi_internal_agent_no,
            pi_internal_agent_name,
            pi_econo_group_code,
            pi_econo_group_name,
            pi_master_policy_no,
            pi_master_begin_date,
            pi_master_end_date,
            pi_epolicy_flag,
            pi_coverdate,
            pi_broker_inx_id,
            pi_brok_comm_perc,
            pi_currency,
            pi_channel,
            pi_office,
            pi_frequency,
            pi_consortium_flag,
            pi_tender_flag,
            pi_billing_type,
            pi_prem_cal_period,
            pi_billing_by,
            pi_issuing_min_prem,
            pi_empl1_rate,
            pi_empl2_rate,
            pi_high_risk1_rate,
            pi_high_risk2_rate,
            pi_low_risk1_rate,
            pi_low_risk2_rate,
            pi_natdeath_sal,
            pi_accdeath_sal,
            pi_itpa_sal,
            pi_plan,
            pi_legal_cov_flag,
            pi_fe_num_sal,
            pi_fe_max_si,
            pi_desg_num_sal,
            pi_desg_max_si,
            pi_homeless_num_sal,
            pi_homeless_max_si,
            pi_anttermill_num_sal,
            pi_anttermill_max_si,
            pi_cancer_death_num_sal,
            pi_cancer_death_max_si,
            pi_cancer_num_sal,
            pi_cancer_max_si,
            pi_critmyo_num_sal,
            pi_critmyo_max_si,
            pi_cistroke_num_sal,
            pi_cistroke_max_si,
            pi_cicrf_num_sal,
            pi_cicrf_max_si,
            pi_cimultscl_num_sal,
            pi_cimultscl_max_si,
            pi_cicoma_num_sal,
            pi_cicoma_max_si,
            pi_cibypass_num_sal,
            pi_cibypass_max_si,
            pi_critill_num_sal,
            pi_critill_max_si,
            pi_blindness_num_sal,
            pi_blindness_max_si,
            pi_critburn_num_sal,
            pi_critburn_max_si,
            pi_posthum_child_num_sal,
            pi_posthum_child_max_si,
            pi_deafness_num_sal,
            pi_deafness_max_si,
            pi_fam_sal_perc,
            pi_fam_num_sal,
            pi_fam_max_si,
            pi_reprem_num_sal,
            pi_reprem_max_si,
            pi_inabwork_num_sal,
            pi_inabwork__max_si,
            pi_transfer_num_sal,
            pi_transfer_max_si,
            pi_unid_policy_flag,
--            pi_legal_limit_flag,
            pi_legal_limit_clause_flag,
            pi_no_salary_limit_flag,
            pi_indem_pay_clause_flag,
            pi_claim_pay_clause_flag,
            pi_currency_clause_flag,
            pi_waiting_clause_flag,
            pi_special_clause_text
        );
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            putlog(pi_stag_id, v_id, '--upload_file_data'||SQLERRM);
--            DECLARE
--                v_code  NUMBER;
--                v_errm  VARCHAR(150);
--            BEGIN
--                v_code  := sqlcode;
--                v_errm  := substr(sqlerrm, 1, 150);
--                insis_cust_lpv.sys_schema_utils.update_poller_process_status(p_ctrl_id, 'ERROR(' || v_code || '): ' ||
--                v_errm);
--
--                insert_error_stg2(p_ctrl_id, NULL, NULL, 8, 'fa_cust_migr_vlt_mp.upload_fa_cust_migr_vlt_mp' || '(' || $$plsql_line ||
--                ')',
--                                  sqlerrm, pio_errmsg);
--
--            END;
    END upload_file_data;


    PROCEDURE vley_wrapper (
        pi_sys_ctrl_id  IN  NUMBER,
        pi_file_id      IN  NUMBER,
        pi_file_name    IN  VARCHAR2
    ) IS

        v_file_id                 NUMBER;
        v_all_succeded            NUMBER;
        v_exito                   VARCHAR2(10);
        l_srverrmsg               insis_sys_v10.srverrmsg;
        v_errm                    VARCHAR(4000);
        l_validation_id           NUMBER;
        l_insis_product_code_rev  VARCHAR2(1);
        l_sales_channel_code_rev  insis_cust.hst_cust_sales_unit.ID%TYPE;
        l_office_number_rev       insis_people_v10.p_offices.office_no%TYPE;
        l_currency_code_rev       VARCHAR2(3);
        l_calculation_type_rev    insis_gen_v10.hs_cond_dimension.ID%TYPE;
        l_billing_type_rev        insis_gen_v10.hs_cond_dimension.ID%TYPE;
        l_billing_way_rev         insis_gen_v10.hs_cond_dimension.ID%TYPE;
        l_policy_holder_code_rev  insis_people_v10.p_clients.client_id%TYPE;
        l_broker_code_rev         insis_people_v10.p_agents.agent_id%TYPE;
        l_as_is_product_code_rev  insis_gen_v10.hs_cond_dimension.ID%TYPE;
        l_agent_type              insis_people_v10.p_agents.agent_type%TYPE;
        pio_err                   srverr;
        v_count_errors            NUMBER;
        --JOB
        l_number_of_jobs          NUMBER := 6;
        l_job_size                NUMBER;
        newjobarr                 sys.job_definition_array;
        l_task_name               VARCHAR(1000);
        l_sql_stmt                VARCHAR2(1000);
        l_n_job                   NUMBER := 0;
        newjob                    sys.job_definition;
        l_job_name                VARCHAR(1000);
        l_jobs_running            NUMBER;

        pio_errmsg                srverr;

        CURSOR c_job_size (
            pi_num_jobs IN NUMBER
        ) IS
        SELECT
            round(COUNT(*) / pi_num_jobs) AS jobs_size
        FROM
            cust_migration.fa_migr_vlt_mp_pol
        WHERE
            control_id = pi_sys_ctrl_id;

    BEGIN
        l_log_proc := pi_sys_ctrl_id;
        putlog(pi_sys_ctrl_id, 0, 'vley_wrapper|start|params: ' || pi_sys_ctrl_id || ',' || pi_file_id || ',' || pi_file_name);

        IF pi_file_id IS NULL THEN
            insis_cust_lpv.sys_schema_utils.log_poller_process(pi_sys_ctrl_id, pi_file_name, lc_poller_name, 'Poller with Process ID ' || pi_sys_ctrl_id,
            v_file_id);
        ELSE
            v_file_id := pi_file_id;
        END IF;

        UPDATE insis_cust_lpv.sys_poller_process_ctrl
        SET
            file_id = v_file_id
        WHERE
            sys_poller_process_ctrl_id = pi_sys_ctrl_id;

        v_all_succeded  := 1;

        putlog(pi_sys_ctrl_id, 0, 'vley_wrapper|validations');

        FOR rec_stag_data IN (
            SELECT
                *
            FROM
                cust_migration.fa_migr_vlt_mp_pol
            WHERE
                control_id = pi_sys_ctrl_id
        ) LOOP
            v_exito := 'OK';


            IF rec_stag_data.internal_agent_no IS NULL THEN
                v_exito := 'ERR';
                l_err_seq := l_err_seq + 1;
                ins_error_stg(
                            pi_sys_ctrl_id  => pi_sys_ctrl_id,
                            pi_stg_id       => rec_stag_data.stag_id,
                            pi_errseq       => l_err_seq,
                            pi_errtype      => 'ERR',
                            pi_errcode      => NULL,
                            pi_errmess      => 'internal_agent_no es nulo',
                            pio_errmsg      => pio_errmsg
                        );

            END IF;

            IF NOT (rec_stag_data.brok_comm_perc = 0) THEN
                v_exito := 'ERR';
                l_err_seq := l_err_seq + 1;
                ins_error_stg(
                            pi_sys_ctrl_id  => pi_sys_ctrl_id,
                            pi_stg_id       => rec_stag_data.stag_id,
                            pi_errseq       => l_err_seq,
                            pi_errtype      => 'ERR',
                            pi_errcode      => NULL,
                            pi_errmess      => 'broker_comm debe ser 0',
                            pio_errmsg      => pio_errmsg
                        );

            END IF;

            BEGIN
                SELECT pa.agent_type
                       INTO l_agent_type
                FROM insis_cust.intrf_lpv_people_ids lc,
                   insis_people_v10.p_agents pa
                WHERE lc.man_id = pa.man_id
                AND lc.insunix_code = rec_stag_data.BROKER_INX_ID;
            

            IF
                  rec_stag_data.channel = 3
            THEN --DIRECTOS
                  IF
                        l_agent_type <> 1 OR rec_stag_data.BROKER_INX_ID <> 'N0000202587'
                  THEN
                        v_exito     := 'ERR';
                        l_err_seq   := l_err_seq + 1;
                        ins_error_stg(
                              pi_sys_ctrl_id   => pi_sys_ctrl_id,
                              pi_stg_id        => rec_stag_data.stag_id,
                              pi_errseq        => l_err_seq,
                              pi_errtype       => 'ERR',
                              pi_errcode       => NULL,
                              pi_errmess       => 'Para Canal de venta Directos el agente debe ser Directo',
                              pio_errmsg       => pio_errmsg
                        );
            
                  END IF;
            end if;
            
            if
                  rec_stag_data.channel = 1
            THEN --BROKER
                  IF
                        l_agent_type <> 5
                  then
                        v_exito     := 'ERR';
                        l_err_seq   := l_err_seq + 1;
                        ins_error_stg(
                              pi_sys_ctrl_id   => pi_sys_ctrl_id,
                              pi_stg_id        => rec_stag_data.stag_id,
                              pi_errseq        => l_err_seq,
                              pi_errtype       => 'ERR',
                              pi_errcode       => null,
                              pi_errmess       => 'Para Canal de venta BROKER el agente debe ser Broker',
                              pio_errmsg       => pio_errmsg
                        );
            
                  END IF;
            END IF;  
            EXCEPTION
            WHEN NO_DATA_FOUND THEN
                l_agent_type := NULL;
                --putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id, '--update_conditions.err:'||SQLERRM);
                v_exito     := 'ERR';
                l_err_seq   := l_err_seq + 1;
                ins_error_stg(
                          pi_sys_ctrl_id   => pi_sys_ctrl_id,
                          pi_stg_id        => rec_stag_data.stag_id,
                          pi_errseq        => l_err_seq,
                          pi_errtype       => 'ERR',
                          pi_errcode       => NULL,
                          pi_errmess       => 'BROKER_INX_ID no tiene equivalencia en intrf_lpv_people_ids',
                          pio_errmsg       => pio_errmsg
                        );
            END;

            IF rec_stag_data.NATDEATH_SAL < 16 THEN
                v_exito := 'ERR';
                l_err_seq := l_err_seq + 1;
                ins_error_stg(
                            pi_sys_ctrl_id  => pi_sys_ctrl_id,
                            pi_stg_id       => rec_stag_data.stag_id,
                            pi_errseq       => l_err_seq,
                            pi_errtype      => 'ERR',
                            pi_errcode      => NULL,
                            pi_errmess      => 'No. Remuneraciones por Muerte Natural es mayor o igual a 16',
                            pio_errmsg      => pio_errmsg
                        );

            END IF;

            IF rec_stag_data.ACCDEATH_SAL < 32 THEN
                v_exito := 'ERR';
                l_err_seq := l_err_seq + 1;
                ins_error_stg(
                            pi_sys_ctrl_id  => pi_sys_ctrl_id,
                            pi_stg_id       => rec_stag_data.stag_id,
                            pi_errseq       => l_err_seq,
                            pi_errtype      => 'ERR',
                            pi_errcode      => NULL,
                            pi_errmess      => 'No. Remuneraciones por Muerte Accidental es mayor o igual a 32',
                            pio_errmsg      => pio_errmsg
                        );

            END IF;

            IF rec_stag_data.ITPA_SAL < 32 THEN
                v_exito := 'ERR';
                l_err_seq := l_err_seq + 1;
                ins_error_stg(
                            pi_sys_ctrl_id  => pi_sys_ctrl_id,
                            pi_stg_id       => rec_stag_data.stag_id,
                            pi_errseq       => l_err_seq,
                            pi_errtype      => 'ERR',
                            pi_errcode      => NULL,
                            pi_errmess      => 'No. Remuneraciones por ITPA es mayor o igual a 32',
                            pio_errmsg      => pio_errmsg
                        );

            end if;
            
            if rec_stag_data.as_is <> '1' OR rec_stag_data.as_is is null then
                v_exito := 'ERR';
                l_err_seq := l_err_seq + 1;
                ins_error_stg(
                            pi_sys_ctrl_id  => pi_sys_ctrl_id,
                            pi_stg_id       => rec_stag_data.stag_id,
                            pi_errseq       => l_err_seq,
                            pi_errtype      => 'ERR',
                            pi_errcode      => null,
                            pi_errmess      => 'as_is no existe',
                            pio_errmsg      => pio_errmsg
                        );
            end if;
            
            if rec_stag_data.policy_state not in (-2,0) OR rec_stag_data.policy_state is null then
                v_exito := 'ERR';
                l_err_seq := l_err_seq + 1;
                ins_error_stg(
                            pi_sys_ctrl_id  => pi_sys_ctrl_id,
                            pi_stg_id       => rec_stag_data.stag_id,
                            pi_errseq       => l_err_seq,
                            pi_errtype      => 'ERR',
                            pi_errcode      => null,
                            pi_errmess      => 'policy_state no existe',
                            pio_errmsg      => pio_errmsg
                        );
            end if;
            
            if rec_stag_data.epolicy_flag not in ('Y','N') OR rec_stag_data.epolicy_flag is null then
                v_exito := 'ERR';
                l_err_seq := l_err_seq + 1;
                ins_error_stg(
                            pi_sys_ctrl_id  => pi_sys_ctrl_id,
                            pi_stg_id       => rec_stag_data.stag_id,
                            pi_errseq       => l_err_seq,
                            pi_errtype      => 'ERR',
                            pi_errcode      => null,
                            pi_errmess      => 'epolicy_flag (Póliza electrónica) no existe',
                            pio_errmsg      => pio_errmsg
                        );
            end if;
            
            if rec_stag_data.currency not in ('PEN','USD') OR rec_stag_data.currency is null then
                v_exito := 'ERR';
                l_err_seq := l_err_seq + 1;
                ins_error_stg(
                            pi_sys_ctrl_id  => pi_sys_ctrl_id,
                            pi_stg_id       => rec_stag_data.stag_id,
                            pi_errseq       => l_err_seq,
                            pi_errtype      => 'ERR',
                            pi_errcode      => null,
                            pi_errmess      => 'currency (Moneda) no existe',
                            pio_errmsg      => pio_errmsg
                        );
            end if;
            
            if rec_stag_data.channel not in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22) OR rec_stag_data.channel is null then
                v_exito := 'ERR';
                l_err_seq := l_err_seq + 1;
                ins_error_stg(
                            pi_sys_ctrl_id  => pi_sys_ctrl_id,
                            pi_stg_id       => rec_stag_data.stag_id,
                            pi_errseq       => l_err_seq,
                            pi_errtype      => 'ERR',
                            pi_errcode      => null,
                            pi_errmess      => 'channel (Canal) no existe',
                            pio_errmsg      => pio_errmsg
                        );
            end if;
            
            if rec_stag_data.office not in (1,2,3,4,5,6,9,12,13,14,15,18,19,24,26,32,33,34,35,37,43,48,49,55,56,57,58,60,62,68,76,77,78,81,82,83,100,101) OR rec_stag_data.office is null then
                v_exito := 'ERR';
                l_err_seq := l_err_seq + 1;
                ins_error_stg(
                            pi_sys_ctrl_id  => pi_sys_ctrl_id,
                            pi_stg_id       => rec_stag_data.stag_id,
                            pi_errseq       => l_err_seq,
                            pi_errtype      => 'ERR',
                            pi_errcode      => null,
                            pi_errmess      => 'office (Oficina) no existe',
                            pio_errmsg      => pio_errmsg
                        );
            end if;

            if rec_stag_data.frequency not in (0,1,2,3,4,6,12) OR rec_stag_data.frequency is null then
                v_exito := 'ERR';
                l_err_seq := l_err_seq + 1;
                ins_error_stg(
                            pi_sys_ctrl_id  => pi_sys_ctrl_id,
                            pi_stg_id       => rec_stag_data.stag_id,
                            pi_errseq       => l_err_seq,
                            pi_errtype      => 'ERR',
                            pi_errcode      => null,
                            pi_errmess      => 'frequency (Frecuencia de pago) no existe',
                            pio_errmsg      => pio_errmsg
                        );
            end if;
            
            if rec_stag_data.consortium_flag not in ('Y','N') OR  rec_stag_data.consortium_flag is null then
                v_exito := 'ERR';
                l_err_seq := l_err_seq + 1;
                ins_error_stg(
                            pi_sys_ctrl_id  => pi_sys_ctrl_id,
                            pi_stg_id       => rec_stag_data.stag_id,
                            pi_errseq       => l_err_seq,
                            pi_errtype      => 'ERR',
                            pi_errcode      => null,
                            pi_errmess      => 'consortium_flag (Póliza consorcio) no existe',
                            pio_errmsg      => pio_errmsg
                        );
            end if;
            
            if rec_stag_data.tender_flag not in ('Y','N') OR rec_stag_data.tender_flag  is null then
                v_exito := 'ERR';
                l_err_seq := l_err_seq + 1;
                ins_error_stg(
                            pi_sys_ctrl_id  => pi_sys_ctrl_id,
                            pi_stg_id       => rec_stag_data.stag_id,
                            pi_errseq       => l_err_seq,
                            pi_errtype      => 'ERR',
                            pi_errcode      => null,
                            pi_errmess      => 'tender_flag (Póliza licitación) no existe',
                            pio_errmsg      => pio_errmsg
                        );
            end if;
            
            if rec_stag_data.billing_type not in (1,2,3) OR rec_stag_data.billing_type is null then
                v_exito := 'ERR';
                l_err_seq := l_err_seq + 1;
                ins_error_stg(
                            pi_sys_ctrl_id  => pi_sys_ctrl_id,
                            pi_stg_id       => rec_stag_data.stag_id,
                            pi_errseq       => l_err_seq,
                            pi_errtype      => 'ERR',
                            pi_errcode      => null,
                            pi_errmess      => 'billing_type (Tipo de facturación) no existe',
                            pio_errmsg      => pio_errmsg
                        );
            end if;
            
            if rec_stag_data.prem_cal_period not in (1,2,3) OR rec_stag_data.prem_cal_period is null then
                v_exito := 'ERR';
                l_err_seq := l_err_seq + 1;
                ins_error_stg(
                            pi_sys_ctrl_id  => pi_sys_ctrl_id,
                            pi_stg_id       => rec_stag_data.stag_id,
                            pi_errseq       => l_err_seq,
                            pi_errtype      => 'ERR',
                            pi_errcode      => null,
                            pi_errmess      => 'prem_cal_period (Tipo cálculo de prima) no existe',
                            pio_errmsg      => pio_errmsg
                        );
            end if;
            
            if rec_stag_data.billing_by not in (1,2) OR rec_stag_data.billing_by is null then
                v_exito := 'ERR';
                l_err_seq := l_err_seq + 1;
                ins_error_stg(
                            pi_sys_ctrl_id  => pi_sys_ctrl_id,
                            pi_stg_id       => rec_stag_data.stag_id,
                            pi_errseq       => l_err_seq,
                            pi_errtype      => 'ERR',
                            pi_errcode      => null,
                            pi_errmess      => 'billing_by (Facturación por)  no existe',
                            pio_errmsg      => pio_errmsg
                        );
            end if;
            
            if rec_stag_data.plan not in (1,2,3,4,5,6,7) OR rec_stag_data.plan is null then
                v_exito := 'ERR';
                l_err_seq := l_err_seq + 1;
                ins_error_stg(
                            pi_sys_ctrl_id  => pi_sys_ctrl_id,
                            pi_stg_id       => rec_stag_data.stag_id,
                            pi_errseq       => l_err_seq,
                            pi_errtype      => 'ERR',
                            pi_errcode      => null,
                            pi_errmess      => 'PLAN no existe',
                            pio_errmsg      => pio_errmsg
                        );
            end if;
            
            if rec_stag_data.legal_cov_flag not in ('Y','N') OR rec_stag_data.legal_cov_flag is null then                
                v_exito := 'ERR';
                l_err_seq := l_err_seq + 1;
                ins_error_stg(
                            pi_sys_ctrl_id  => pi_sys_ctrl_id,
                            pi_stg_id       => rec_stag_data.stag_id,
                            pi_errseq       => l_err_seq,
                            pi_errtype      => 'ERR',
                            pi_errcode      => null,
                            pi_errmess      => 'legal_cov_flag (Pólizas solo con coberturas de ley) no existe',
                            pio_errmsg      => pio_errmsg
                        );
            end if;
            
            if rec_stag_data.unid_policy_flag is null OR rec_stag_data.unid_policy_flag not in ('Y','N') then                
                v_exito := 'ERR';
                l_err_seq := l_err_seq + 1;
                ins_error_stg(
                            pi_sys_ctrl_id  => pi_sys_ctrl_id,
                            pi_stg_id       => rec_stag_data.stag_id,
                            pi_errseq       => l_err_seq,
                            pi_errtype      => 'ERR',
                            pi_errcode      => null,
                            pi_errmess      => 'unid_policy_flag (Póliza No Nominativa) no existe',
                            pio_errmsg      => pio_errmsg
                        );
            end if;
            
            if rec_stag_data.legal_limit_clause_flag is null OR rec_stag_data.legal_limit_clause_flag not in ('Y','N') then                
                v_exito := 'ERR';
                l_err_seq := l_err_seq + 1;
                ins_error_stg(
                            pi_sys_ctrl_id  => pi_sys_ctrl_id,
                            pi_stg_id       => rec_stag_data.stag_id,
                            pi_errseq       => l_err_seq,
                            pi_errtype      => 'ERR',
                            pi_errcode      => null,
                            pi_errmess      => 'legal_limit_clause_flag - Cláusula Tope de Ley (608) no existe',
                            pio_errmsg      => pio_errmsg
                        );
            end if;
            
            if rec_stag_data.no_salary_limit_flag is null OR rec_stag_data.no_salary_limit_flag not in ('Y','N') then                
                v_exito := 'ERR';
                l_err_seq := l_err_seq + 1;
                ins_error_stg(
                            pi_sys_ctrl_id  => pi_sys_ctrl_id,
                            pi_stg_id       => rec_stag_data.stag_id,
                            pi_errseq       => l_err_seq,
                            pi_errtype      => 'ERR',
                            pi_errcode      => null,
                            pi_errmess      => 'no_salary_limit_flag - Cláusula Sin Tope de Ley (601) no existe',
                            pio_errmsg      => pio_errmsg
                        );
            end if;
            
              if (rec_stag_data.no_salary_limit_flag = 'Y' and rec_stag_data.legal_limit_clause_flag = 'Y') or (rec_stag_data.no_salary_limit_flag = 'N' and rec_stag_data.legal_limit_clause_flag = 'N') then                
                v_exito := 'ERR';
                l_err_seq := l_err_seq + 1;
                ins_error_stg(
                            pi_sys_ctrl_id  => pi_sys_ctrl_id,
                            pi_stg_id       => rec_stag_data.stag_id,
                            pi_errseq       => l_err_seq,
                            pi_errtype      => 'ERR',
                            pi_errcode      => null,
                            pi_errmess      => 'Ambas clausulas (608,601) tienen el mismo valor',
                            pio_errmsg      => pio_errmsg
                        );
            end if;
            
            if rec_stag_data.INDEM_PAY_CLAUSE_FLAG is null OR rec_stag_data.INDEM_PAY_CLAUSE_FLAG not in ('Y','N') then                
                v_exito := 'ERR';
                l_err_seq := l_err_seq + 1;
                ins_error_stg(
                            pi_sys_ctrl_id  => pi_sys_ctrl_id,
                            pi_stg_id       => rec_stag_data.stag_id,
                            pi_errseq       => l_err_seq,
                            pi_errtype      => 'ERR',
                            pi_errcode      => null,
                            pi_errmess      => 'INDEM_PAY_CLAUSE_FLAG - Cláusula Pago de indemnizaciones (602) no existe',
                            pio_errmsg      => pio_errmsg
                        );
            end if;
            
            if rec_stag_data.CLAIM_PAY_CLAUSE_FLAG is null OR rec_stag_data.CLAIM_PAY_CLAUSE_FLAG not in ('Y','N') then                
                v_exito := 'ERR';
                l_err_seq := l_err_seq + 1;
                ins_error_stg(
                            pi_sys_ctrl_id  => pi_sys_ctrl_id,
                            pi_stg_id       => rec_stag_data.stag_id,
                            pi_errseq       => l_err_seq,
                            pi_errtype      => 'ERR',
                            pi_errcode      => null,
                            pi_errmess      => 'CLAIM_PAY_CLAUSE_FLAG - Cláusula Plazo de pago de Siniestros (603) no existe',
                            pio_errmsg      => pio_errmsg
                        );
            end if;
            
            if rec_stag_data.currency_clause_flag is null OR rec_stag_data.currency_clause_flag not in ('Y','N') then                
                v_exito := 'ERR';
                l_err_seq := l_err_seq + 1;
                ins_error_stg(
                            pi_sys_ctrl_id  => pi_sys_ctrl_id,
                            pi_stg_id       => rec_stag_data.stag_id,
                            pi_errseq       => l_err_seq,
                            pi_errtype      => 'ERR',
                            pi_errcode      => null,
                            pi_errmess      => 'CURRENCY_CLAUSE_FLAG - Cláusula Moneda (605) no existe',
                            pio_errmsg      => pio_errmsg
                        );
            end if;
            
            if rec_stag_data.waiting_clause_flag is null OR rec_stag_data.waiting_clause_flag not in ('Y','N') then                
                v_exito := 'ERR';
                l_err_seq := l_err_seq + 1;
                ins_error_stg(
                            pi_sys_ctrl_id  => pi_sys_ctrl_id,
                            pi_stg_id       => rec_stag_data.stag_id,
                            pi_errseq       => l_err_seq,
                            pi_errtype      => 'ERR',
                            pi_errcode      => null,
                            pi_errmess      => 'WAITING_CLAUSE_FLAG - Cláusula Periodo de carencia (606) no existe',
                            pio_errmsg      => pio_errmsg
                        );
            end if;
            
            if (rec_stag_data.EMPL1_RATE is null and rec_stag_data.EMPL2_RATE is null and rec_stag_data.HIGH_RISK1_RATE is null and rec_stag_data.HIGH_RISK2_RATE is null and rec_stag_data.low_risk1_rate is null and rec_stag_data.LOW_RISK2_RATE is null) then                
                v_exito := 'ERR';
                l_err_seq := l_err_seq + 1;
                ins_error_stg(
                            pi_sys_ctrl_id  => pi_sys_ctrl_id,
                            pi_stg_id       => rec_stag_data.stag_id,
                            pi_errseq       => l_err_seq,
                            pi_errtype      => 'ERR',
                            pi_errcode      => null,
                            pi_errmess      => 'No tiene Tasa',
                            pio_errmsg      => pio_errmsg
                        );
            end if;


            IF v_exito = 'OK' THEN
                UPDATE cust_migration.fa_migr_vlt_mp_pol
                SET
                    att_status_rec = LC_STAT_REC_VALID
                WHERE
                    control_id = rec_stag_data.control_id
                AND stag_id = rec_stag_data.stag_id;

            ELSE
                UPDATE cust_migration.fa_migr_vlt_mp_pol
                SET
                    att_status_rec = LC_STAT_REC_ERROR
                WHERE
                    control_id = rec_stag_data.control_id
                AND stag_id = rec_stag_data.stag_id;

            END IF;

        END LOOP;

        putlog(pi_sys_ctrl_id, 0, 'vley_wrapper|defining jobs');

        OPEN c_job_size(l_number_of_jobs);
        FETCH c_job_size INTO l_job_size;
        CLOSE c_job_size;
        IF l_job_size = 0 THEN
            l_job_size := 1;
        END IF;
        l_task_name     := pi_sys_ctrl_id || '_';

        newjobarr       := sys.job_definition_array();

        putlog(pi_sys_ctrl_id, 0, 'vley_wrapper|starting jobs');
        FOR c_st IN (
            WITH A AS (
                SELECT
                    stag_id,
                    ROW_NUMBER() OVER(PARTITION BY control_id
                        ORDER BY
                            stag_id
                    ) AS rownum_
                FROM
                    cust_migration.fa_migr_vlt_mp_pol
                WHERE
                        control_id = pi_sys_ctrl_id
                    AND att_status_rec = lc_stat_rec_valid
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
            l_sql_stmt             := 'BEGIN cust_migration.fa_cust_migr_vlt_mp.vley_job_proc (' || pi_sys_ctrl_id || ', ' || c_st.fv || ', ' ||
            c_st.lv || ', ' || pi_file_id || ', ' || chr(39) || pi_file_name || chr(39) || '); END;';

            putlog(pi_sys_ctrl_id, 0, 'vley_wrapper|job['|| c_st.page_num ||']: ' || l_sql_stmt);

            l_n_job                := l_n_job + 1;
            l_job_name             := l_task_name || l_n_job;
            newjobarr.EXTEND;

            newjob                 := sys.job_definition(job_name => '"' || l_job_name || '"',
                                                        job_style => 'REGULAR',
                                                        number_of_arguments => 0,
                                                        job_type => 'PLSQL_BLOCK',
                                                        job_action => l_sql_stmt,
                                                        start_date => sysdate,
                                                        enabled => TRUE,
                                                        auto_drop => TRUE,
                                                        comments => 'one-time job');

            newjobarr(l_n_job)     := newjob;
        END LOOP;
        putlog(pi_sys_ctrl_id, 0, 'ley_wrapper|waiting jobs...');

        dbms_scheduler.create_jobs(newjobarr, 'TRANSACTIONAL'); --TRANSACTIONAL STOP_ON_FIRST_ERROR  ABSORB_ERRORS

        dbms_lock.sleep(1);
        WHILE ( TRUE ) LOOP
            SELECT
                COUNT(1)
            INTO l_jobs_running
            FROM
                all_scheduler_running_jobs
            WHERE
                job_name LIKE '%' || l_task_name || '%';

            IF l_jobs_running > 0 THEN

                dbms_lock.sleep(1);
            ELSE
                putlog(pi_sys_ctrl_id, 0, 'vley_wrapper|end_jobs');

                EXIT;
            END IF;
        END LOOP;

        putlog(pi_sys_ctrl_id, 0, 'vley_wrapper|checking_errors...');

        SELECT
            COUNT(*)
        INTO v_count_errors
        FROM
            cust_migration.fa_migr_vley_err
        WHERE
            control_id = pi_sys_ctrl_id;

        putlog(pi_sys_ctrl_id, 0, 'vley_wrapper|count:'||v_count_errors);
        IF v_count_errors > 0 THEN
            insis_cust_lpv.sys_schema_utils.update_poller_process_status(pi_sys_ctrl_id, 'ERROR');
        ELSE
            insis_cust_lpv.sys_schema_utils.update_poller_process_status(pi_sys_ctrl_id, 'SUCCESS');
        END IF;


        --creates record for report process
        INSERT INTO cust_migration.fa_migr_vley_err (
                    CONTROL_ID,
                    STAG_ID,
                    ERRSEQ,
                    ERRTYPE,
                    ERRCODE,
                    ERRMESS
        ) VALUES (
            pi_sys_ctrl_id,
            0,
            0,
            'REP',
            NULL,
            '--Record ready for report--'
        );

        COMMIT;

        putlog(pi_sys_ctrl_id, 0, 'vley_wrapper|end');

    EXCEPTION
        WHEN OTHERS THEN
            putlog(pi_sys_ctrl_id, 0, 'vley_wrapper|end_error');
--            insert_error_stg2(pi_sys_ctrl_id, NULL, NULL, 8, 'fa_cust_migr_vlt_mp.fa_cust_migr_vlt_mp_wrapper' || '(' || $$plsql_line || ')',
--                              sqlerrm, pio_err);
    END vley_wrapper;


    PROCEDURE vley_job_proc (
        pi_sys_ctrl_id  IN  NUMBER,
        pi_stg_init     IN  NUMBER,
        pi_stg_end      IN  NUMBER,
        pi_file_id      IN  NUMBER,
        pi_file_name    IN  VARCHAR2
    ) IS

        pio_err        srverr;
        v_errm         VARCHAR2(4000);
        v_file_id      NUMBER;
        v_stat_poller  VARCHAR(500);
        pio_errmsg     srverr;
    BEGIN
        v_stat_poller := 'OK';
        l_log_proc := pi_sys_ctrl_id;

        putlog(pi_sys_ctrl_id, 0, 'vley_job_proc|start');

--        SELECT
--            status
--        INTO v_stat_poller
--        FROM
--            insis_cust_lpv.sys_poller_process_ctrl
--        WHERE
--            sys_poller_process_ctrl_id = sys_ctrl_id;

        IF v_stat_poller <> 'ERROR' THEN
            FOR rec_staging_data IN (
                SELECT
                    *
                FROM
                    cust_migration.fa_migr_vlt_mp_pol
                WHERE
                        control_id = pi_sys_ctrl_id
                    AND stag_id BETWEEN pi_stg_init AND pi_stg_end
                    AND att_status_rec = LC_STAT_REC_VALID
            ) LOOP
                SAVEPOINT process_data_sp;
                pio_errmsg := NEW srverr();

                vley_record_proc(rec_staging_data, pio_errmsg);

                IF insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
--                    UPDATE cust_migration.fa_migr_vlt_mp_pol
--                    SET
--                        att_status_rec = lc_stat_rec_valid
--                    WHERE
--                          control_id = pi_sys_ctrl_id
--                      and stag_id = rec_staging_data.stag_id;
--
                    COMMIT;
                ELSE
--todo:activar tras pruebas
--
--                    ROLLBACK TO process_data_sp;
--

                    UPDATE cust_migration.fa_migr_vlt_mp_pol
                    SET
                        att_status_rec = LC_STAT_REC_ERROR
                    WHERE
                          control_id = pi_sys_ctrl_id
                      AND stag_id = rec_staging_data.stag_id;

                    --todo: assign seq according job number (page)
                    --uses same record number by now
--                    l_err_seq := rec_staging_data.stag_id;
                    l_err_seq := l_err_seq + 1;
                    ins_error_stg(
                        pi_sys_ctrl_id  => pi_sys_ctrl_id,
                        pi_stg_id       => rec_staging_data.stag_id,
                        pi_errseq       => l_err_seq,
                        pi_errtype      => 'ERR',
                        pi_errcode      => NULL,
                        pi_errmess      => srv_error.ErrCollection2String(pio_errmsg),
                        pio_errmsg      => pio_errmsg
                    );

                    COMMIT;
                END IF;

            END LOOP;
        ELSE
            insis_cust_lpv.sys_schema_utils.update_poller_process_status(pi_sys_ctrl_id, 'ERROR');
        END IF;

        putlog(pi_sys_ctrl_id, 0, 'vley_job_proc|end');

    EXCEPTION
        WHEN OTHERS THEN
            putlog(pi_sys_ctrl_id, 0, 'vley_job_proc|end-err|'||SQLERRM);

--            ins_error_stg(pi_sys_ctrl_id, 0, 0, 8, 'fa_cust_migr_vlt_mp.process_fa_cust_migr_vlt_mp' || '(' || $$plsql_line || ')',
--                              sqlerrm, pio_err);
    END vley_job_proc;

    PROCEDURE vley_record_proc (
        pi_fa_vley_row       IN      cust_migration.fa_migr_vlt_mp_pol%ROWTYPE,
        pio_errmsg           IN OUT  srverr
    ) IS

        v_code                   VARCHAR(4000);
        v_errm                   VARCHAR(4000);
        l_outcontext             srvcontext;
        l_client_id              insis_people_v10.p_clients.client_id%TYPE;
        l_office_id              insis_people_v10.p_offices.office_id%TYPE;
        l_engagement_id          insis_gen_v10.policy_engagement.engagement_id%TYPE;
        l_agent_id               insis_people_v10.p_agents.agent_id%TYPE;
        l_agent_id_directos      insis_people_v10.p_agents.agent_id%TYPE;
        l_agent_type             insis_people_v10.p_agents.agent_type%TYPE;
        l_internal_agent_type    insis_people_v10.pp_agent_type;
        l_internal_agent_id      insis_people_v10.p_agents.agent_id%TYPE;
        l_master_policy_id       insis_gen_v10.POLICY.policy_id%TYPE;
        calc_duration            NUMBER;
        calc_dimension           VARCHAR2(1);

        TYPE objecttype_table IS TABLE OF varchar2(100) INDEX BY PLS_INTEGER;
        l_obj_type_table        objecttype_table;

        l_object_type            NUMBER;
        l_parent_obj_type        NUMBER;
        l_parent_obj_type_aux    NUMBER;
        count_parent_obj_type    NUMBER;
        l_description            VARCHAR2(100);
        l_parent_ins_obj_id      NUMBER;
        l_ins_obj_id             NUMBER;
        l_procedure_result       VARCHAR2(100);
        l_tariff_percent         NUMBER;
        l_begin_date             DATE;
        l_end_date               DATE;
        l_date_covered           DATE;
        l_result                 BOOLEAN;
        l_quest_answer           insis_sys_V10.quest_questions.quest_answer%TYPE;

        l_tech_branch            insis_gen_v10.POLICY.attr1%TYPE;
        l_sbs_code               insis_gen_v10.POLICY.attr2%TYPE;

        PROCEDURE update_quest(
            pi_area         IN varchar2,
            pi_quest_code   IN insis_sys_V10.quest_questions.quest_id%TYPE,
            pi_quest_answer IN insis_sys_V10.quest_questions.quest_answer%TYPE,
            pi_policy_id    IN insis_sys_V10.quest_questions.policy_id%TYPE,
            pi_annex_id     IN insis_sys_V10.quest_questions.annex_id%TYPE,
            pio_outcontext  IN OUT srvcontext,
            pio_errmsg      IN OUT srverr)
        AS

        BEGIN
            putlog(pi_fa_vley_row.control_id, pi_fa_vley_row.stag_id,'--GET_POL_QUEST|' || pi_quest_code|| ':' || pi_quest_answer);

            --================================================================================================
            --PREPARE INFORMATION FOR GET_POL_QUEST EVENT
            --================================================================================================

            insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_ID', insis_sys_v10.srv_context.integers_format, pi_policy_id);
            insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ANNEX_ID', insis_sys_v10.srv_context.integers_format, pi_annex_id);
            insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'QUEST_CODE', pi_quest_code);

            --================================================================================================
            -- GET_POL_QUEST
            -- Output parameter : srv_question_data.gQuestionRecord/srv_question_data.gQuestionTable
            --================================================================================================
            insis_sys_v10.srv_events.sysevent('GET_POL_QUEST', l_outcontext, l_outcontext, pio_errmsg);
            IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
                putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,
                        '--GET_POL_QUEST.err:'||srv_error.ErrCollection2String(pio_errmsg));

                RETURN;
            END IF;

            --================================================================================================
            --PREPARE INFORMATION FOR UPD_QUEST EVENT
            --================================================================================================

--            putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'--UPD_QUEST' );
            insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ID', insis_sys_v10.srv_context.integers_format, insis_sys_v10.srv_question_data.gquestionrecord.ID);
            insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'QUEST_ANSWER', pi_quest_answer);

            --================================================================================================
            -- UPD_QUEST
            -- Output parameter : srv_question_data.gQuestionRecord/srv_question_data.gQuestionTable
            --================================================================================================

            insis_sys_v10.srv_events.sysevent('UPD_QUEST', l_outcontext, l_outcontext, pio_errmsg);
            IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
                putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,
                        '--UPD_QUEST.err:'||srv_error.ErrCollection2String(pio_errmsg));

                RETURN;
            END IF;

        END update_quest;

        --
        PROCEDURE update_conditions(
            pi_cond_type    IN insis_gen_v10.policy_conditions.cond_type%TYPE,
            pi_cond_dim     IN insis_gen_v10.policy_conditions.cond_dimension%TYPE,
            pi_cond_val     IN insis_gen_v10.policy_conditions.cond_value%TYPE,
            pi_policy_id    IN insis_gen_v10.policy_conditions.policy_id%TYPE,
            pi_annex_id     IN insis_gen_v10.policy_conditions.annex_id%TYPE,
            pio_errmsg      IN OUT srverr)
        AS
        BEGIN
            putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'--update_conditions:'||pi_cond_type);

            UPDATE insis_gen_v10.policy_conditions
            SET
                cond_dimension = pi_cond_dim,
                cond_value = pi_cond_val
            WHERE
                    policy_id = pi_policy_id
                AND annex_id  = pi_annex_id
                AND cond_type = pi_cond_type;

        EXCEPTION
            WHEN OTHERS THEN
                putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,
                        '--update_conditions.err:'||SQLERRM);
                srv_error_set('update_conditions', 'SYSERROR', SQLERRM, pio_errmsg);
        END update_conditions;

        --convert "Y" to numeric value
        FUNCTION yn_to_num(pi_yn varchar2, pi_val_yes PLS_INTEGER, pi_val_no PLS_INTEGER) RETURN varchar2
        AS
        BEGIN
            RETURN CASE WHEN pi_yn = 'Y' THEN pi_val_yes ELSE pi_val_no END;
        END yn_to_num;

        -- get_sbs_techbr. get data from product
        PROCEDURE get_sbs_techbr(
            pi_insr_type   IN  cust_migration.fa_migr_vlt_mp_pol.insr_type%TYPE,
            pi_as_is       IN  cust_migration.fa_migr_vlt_mp_pol.as_is%TYPE,
            po_tech_branch OUT insis_gen_v10.POLICY.attr1%TYPE,
            po_sbs_code    OUT insis_gen_v10.POLICY.attr2%TYPE)
        AS

        BEGIN

            BEGIN
                SELECT technical_branch, sbs_code
                INTO po_tech_branch, po_sbs_code
                FROM insis_cust.CFGLPV_POLICY_TECHBRANCH_SBS
                WHERE insr_type = pi_insr_type
                AND as_is_product = pi_as_is;
            EXCEPTION
                WHEN others THEN
                    po_tech_branch := NULL;
                    po_sbs_code := NULL;
                    putlog(pi_fa_vley_row.control_id, pi_fa_vley_row.stag_id, '--get_sbs_techbr.err:'||SQLERRM);
            END;

        END get_sbs_techbr;

    BEGIN
        l_log_proc := pi_fa_vley_row.control_id || '-' || pi_fa_vley_row.stag_id;
        putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'vley_record_proc|start|'||pi_fa_vley_row.master_policy_no);

        EXECUTE IMMEDIATE 'alter session set NLS_NUMERIC_CHARACTERS = ''.,''';
        insis_sys_v10.insis_context.prepare_session('GEN', NULL, lc_process_user, 'InsisStaff', NULL, NULL);

        --Inicializar variable de contexto

        l_outcontext                         := srvcontext();

--        putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'select client_id');
        BEGIN
            SELECT
                client_id
            INTO l_client_id
            FROM
                insis_people_v10.p_clients
            WHERE
                man_id = (
                    SELECT
                        man_id
                    FROM
                        insis_cust.intrf_lpv_people_ids
                    WHERE
                        insunix_code = pi_fa_vley_row.holder_inx_id
                );

        EXCEPTION
            WHEN OTHERS THEN
                putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'select client_id.err:'||SQLERRM);
                srv_error_set('select client_id', 'PP_Client_Type_invalid_client_id', SQLERRM, pio_errmsg);
                RETURN;
        END;

--        putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'select broker agent_id');
        BEGIN
            SELECT
                agent_id, agent_type
            INTO l_agent_id, l_agent_type
            FROM
                insis_people_v10.p_agents
            WHERE
                man_id = (
                    SELECT
                        man_id
                    FROM
                        insis_cust.intrf_lpv_people_ids
                    WHERE
                        insunix_code = pi_fa_vley_row.broker_inx_id
                );

        EXCEPTION
            WHEN OTHERS THEN
                putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'select agent_id.err:'||SQLERRM);
                srv_error_set('select agent_id', 'InsrDurValidate_Agent', SQLERRM, pio_errmsg);
                RETURN;
        END;
--        putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'broker.agent_id, type:' || l_agent_id || ',' || l_agent_type);

--        putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'select office_id');
        BEGIN
            SELECT
                office_id
            INTO l_office_id
            FROM
                insis_people_v10.p_offices
            WHERE
                office_no = lpad(pi_fa_vley_row.office, 2, '0');

        EXCEPTION
            WHEN OTHERS THEN
                putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'select agent_id.err:'||SQLERRM);
                srv_error_set('select office_id', 'create_office_version_office_id_null_val', SQLERRM, pio_errmsg);
                RETURN;
        END;

        l_internal_agent_type := NEW insis_people_v10.pp_agent_type( pi_fa_vley_row.internal_agent_no);
        IF l_internal_agent_type IS NOT NULL AND
           l_internal_agent_type.agent_id IS NOT NULL THEN
--            putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id, 'internal_agent_id:'||l_internal_agent_type.agent_id);
            l_internal_agent_id := l_internal_agent_type.agent_id;
        ELSE
            putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id, 'pp_agent_type.err:internal_agent_not_found ('||pi_fa_vley_row.internal_agent_no ||')');
            srv_error_set('select internal_agent_no', NULL, 'internal_agent_not_found ('||pi_fa_vley_row.internal_agent_no ||')', pio_errmsg);
            RETURN;
        END IF;

        putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'--CREATE_ENGAGEMENT');

        --================================================================================================
        --PREPARE INFORMATION FOR CREATE_ENGAGEMENT EVENT
        --================================================================================================

        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ENGAGEMENT_ID', insis_sys_v10.srv_context.integers_format, NULL);
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'CLIENT_ID', insis_sys_v10.srv_context.integers_format, l_client_id);
        insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'ENGAGEMENT_STAGE', insis_gen_v10.gvar_pas.at_appl);
        insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'ENGAGEMENT_TYPE', insis_gen_v10.gvar_pas.eng_type_engagement);

        --================================================================================================
        -- CREATE_ENGAGEMENT
        -- Output parameter : ENGAGEMENT_ID
        --================================================================================================
        insis_sys_v10.srv_events.sysevent('CREATE_ENGAGEMENT', l_outcontext, l_outcontext, pio_errmsg);
        IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
            putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,
                    '--CREATE_ENGAGEMENT.err:'||srv_error.ErrCollection2String(pio_errmsg));

            RETURN;
        END IF;

        insis_sys_v10.srv_context.getcontextattrnumber(l_outcontext, 'ENGAGEMENT_ID', l_engagement_id);

        --================================================================================================
        --PREPARE INFORMATION FOR CREATE_ENG_POLICY EVENT
        --================================================================================================

        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ENGAGEMENT_ID', insis_sys_v10.srv_context.integers_format, l_engagement_id);
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'INSR_TYPE', insis_sys_v10.srv_context.integers_format, pi_fa_vley_row.insr_type);
        insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'POLICY_TYPE', insis_gen_v10.gvar_pas.engpoltype_master);
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_ID_ORG', insis_sys_v10.srv_context.integers_format, NULL);
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'AGENT_ID', insis_sys_v10.srv_context.integers_format,l_internal_agent_id);

--        --In case of asesor agent, it is necesary to include "Directos" internal agent
--        IF l_agent_type = 5 THEN
--            putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'select agent_id 1412');
--
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
--                    putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'agent_id1412.err:'||sqlerrm);
--                    srv_error_set('select agent_id_1412', 'InsrDurValidate_Agent', sqlerrm, pio_errmsg);
--
--                    return;
--            END;
--
--            insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'AGENT_ID', insis_sys_v10.srv_context.integers_format, l_agent_id_directos);--DIRECTOS
--
--        ELSE
--            insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'AGENT_ID', insis_sys_v10.srv_context.integers_format, l_agent_id);
--        END IF;

        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_STAGE', insis_sys_v10.srv_context.integers_format, insis_gen_v10.gvar_pas.define_applprep_state);


        --================================================================================================
        -- CREATE_ENG_POLICY
        -- Output parameter : POLICY_ID
        --================================================================================================

        putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'--CREATE_ENG_POLICY');
        insis_sys_v10.srv_events.sysevent('CREATE_ENG_POLICY', l_outcontext, l_outcontext, pio_errmsg);

        IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
            putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,
                   'CREATE_ENG_POLICY.err:'||srv_error.ErrCollection2String(pio_errmsg));
            RETURN;
        END IF;

        insis_sys_v10.srv_context.getcontextattrnumber(l_outcontext, 'POLICY_ID', l_master_policy_id);

        --todo: temporary update ?
        UPDATE cust_migration.fa_migr_vlt_mp_pol
        SET
            att_new_policy_id = l_master_policy_id
        WHERE
                control_id = pi_fa_vley_row.control_id
            AND stag_id = pi_fa_vley_row.stag_id;

        putlog(pi_fa_vley_row.control_id, pi_fa_vley_row.stag_id, 'l_master_policy_id: ' || l_master_policy_id);

        l_begin_date     := tdate(pi_fa_vley_row.master_begin_date); --+ 0.5;
        l_end_date       := tdate(pi_fa_vley_row.master_end_date); -- + 0.5 - 1/24/60/60;
        l_date_covered   := tdate(pi_fa_vley_row.coverdate);


        IF pi_fa_vley_row.econo_group_code IS NOT NULL THEN
--            putlog(pi_fa_vley_row.control_id, pi_fa_vley_row.stag_id, '--INS_POLICY_BENEFIT_GROUPS');
            insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'BENEFIT_GROUP_ID', NULL);
    --        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ENGAGEMENT_ID', insis_sys_v10.srv_context.integers_format, l_engagement_id); --already loaded
    --        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_ID', insis_sys_v10.srv_context.integers_format, l_master_policy_id); --already loaded
            insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ANNEX_ID', insis_sys_v10.srv_context.integers_format, insis_gen_v10.gvar_pas.def_annx_id);
            insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'GROUP_TYPE', lc_group_type_def);
            insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'GROUP_CODE', pi_fa_vley_row.econo_group_code);
            insis_sys_v10.srv_context.setcontextattrdate(l_outcontext, 'VALID_FROM', insis_sys_v10.srv_context.date_format, l_begin_date);
            insis_sys_v10.srv_context.setcontextattrdate(l_outcontext, 'VALID_TO', insis_sys_v10.srv_context.date_format, l_end_date);
            insis_sys_v10.srv_context.setcontextattrdate(l_outcontext, 'REGISTRATION_DATE', insis_sys_v10.srv_context.date_format, l_begin_date);
            insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'USERNAME', lc_policy_user);
            insis_sys_v10.srv_events.sysevent('INS_POLICY_BENEFIT_GROUPS', l_outcontext, l_outcontext, pio_errmsg);

            IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
                putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id, 'ins_policy_benefit_groups.err:'||srv_error.ErrCollection2String(pio_errmsg));
                RETURN;
            END IF;
        END IF;


----        putlogcontext(pi_fa_vley_row.control_id, l_outcontext);
--

        --ISS036-Add BROKER/BROKERVAT agent
        IF l_agent_type = 5 THEN
--            putlog(pi_fa_vley_row.control_id, pi_fa_vley_row.stag_id, '--INS_POLICY_AGENTS.brok');

            insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_AGENT_ID', insis_sys_v10.srv_context.integers_format, NULL);
            insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'AGENT_ID', insis_sys_v10.srv_context.integers_format, l_agent_id);
            insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ANNEX_ID', insis_sys_v10.srv_context.integers_format, insis_gen_v10.gvar_pas.def_annx_id);
            insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'AGENT_ROLE', 'BROK');
            insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'COMM_SHARE', insis_sys_v10.srv_context.integers_format, 100);
            insis_sys_v10.srv_context.setcontextattrdate(l_outcontext, 'VALID_FROM', insis_sys_v10.srv_context.date_format, l_begin_date);

            insis_sys_v10.srv_events.sysevent('INS_POLICY_AGENTS', l_outcontext, l_outcontext, pio_errmsg);

            IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
                putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,
                       'INS_POLICY_AGENTS.err:'||srv_error.ErrCollection2String(pio_errmsg));
                RETURN;
            END IF;

--            putlog(pi_fa_vley_row.control_id, pi_fa_vley_row.stag_id, '--INS_POLICY_AGENTS.brokvat');
            insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_AGENT_ID', insis_sys_v10.srv_context.integers_format, NULL);
            insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'AGENT_ROLE', 'BROKVAT');

            insis_sys_v10.srv_events.sysevent('INS_POLICY_AGENTS', l_outcontext, l_outcontext, pio_errmsg);
            IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
                putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,
                       'INS_POLICY_AGENTS.err:'||srv_error.ErrCollection2String(pio_errmsg));
                RETURN;
            END IF;

        END IF;

--
        putlog(pi_fa_vley_row.control_id, pi_fa_vley_row.stag_id, '--calcduration:' ||pi_fa_vley_row.prem_cal_period || ','|| l_begin_date || '-' || l_end_date);
----        insis_gen_v10.pol_values.calcduration(l_begin_date, l_end_date, pi_fa_vley_row.insis_product_code, calc_duration, calc_dimension);
----        insis_gen_v10.pol_values.CalcDuration_YMD(l_begin_date, l_end_date, pi_fa_vley_row.insis_product_code, calc_duration, calc_dimension);
----        pol_ps_cons.covobjduration
--
        --For special period, duration is set to days
        IF pi_fa_vley_row.prem_cal_period = 0 THEN
            calc_dimension := GVAR_PAS.DUR_DIM_D;
            calc_duration  := l_end_date - l_begin_date;
        ELSE
            insis_gen_v10.pol_values.calcduration(l_begin_date, l_end_date, pi_fa_vley_row.insr_type, calc_duration, calc_dimension);
        END IF;


        IF pi_fa_vley_row.frequency > 0 AND
           add_months(l_begin_date, pi_fa_vley_row.frequency) > l_end_date THEN
            putlog(pi_fa_vley_row.control_id, pi_fa_vley_row.stag_id, 'Master Premium Period > Policy duration');

            srv_error_set('Master Premium Period', NULL, 'Master Premium Period > Policy duration', pio_errmsg);

            RETURN;

        END IF;

        putlog(pi_fa_vley_row.control_id, pi_fa_vley_row.stag_id, 'update policy:' || calc_duration ||  ','|| calc_dimension);

        --ISS050--Add policy and product codes
        get_sbs_techbr(pi_fa_vley_row.insr_type, pi_fa_vley_row.as_is, l_tech_branch, l_sbs_code);

        --todo: usa ojbect type
        UPDATE insis_gen_v10.POLICY
        SET
            policy_no = pi_fa_vley_row.master_policy_no,
            policy_name = pi_fa_vley_row.master_policy_no,
            insr_begin = l_begin_date,
            insr_end = l_end_date,
            date_given = l_begin_date,
            date_covered = l_date_covered,
            conclusion_date = l_begin_date,
            insr_duration = calc_duration,
            dur_dimension = calc_dimension,
            payment_duration = calc_duration,
            payment_dur_dim = calc_dimension,
            attr1 = l_tech_branch,
            attr2 = l_sbs_code,
            attr3 = pi_fa_vley_row.channel,
            attr4 = l_office_id,
            --ISSS047-Fix Master Premium Period
            attr5 = pi_fa_vley_row.frequency,
            payment_type = 'S', --single premium
            username = lc_policy_user
        WHERE
            policy_id = l_master_policy_id;


        --================================================================================================
        -- Updating policy_engagement_billing
        --================================================================================================

        --fill policyengagementbilling structure
        insis_gen_v10.srv_engagement_ds.get_policyengbillingbypolicy(l_outcontext,l_outcontext,pio_errmsg);

        IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
            putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,
                   'get_policyengbillingbypolicy.err:'||srv_error.ErrCollection2String(pio_errmsg));
            RETURN;
        END IF;

----        putlogcontext(pi_fa_vley_row.control_id, l_outcontext);
----        putlog(pi_fa_vley_row.control_id, pi_fa_vley_row.stag_id,
----               'eng_bill_id:'||insis_gen_v10.srv_policy_data.gengagementbillingrecord.engagement_id || '/' || insis_gen_v10.srv_policy_data.gengagementbillingrecord.num_instalments_period);

        --assign fixed value for SCTR
        insis_gen_v10.srv_policy_data.gengagementbillingrecord.num_instalments_period := insis_gen_v10.gvar_pas.instalments_period_policy;
        --update
        l_result := insis_gen_v10.srv_policy_data.gengagementbillingrecord.updatepengagementbilling(pio_errmsg);

        IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
            putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,
                   'updatepengagementbilling.err:'||srv_error.ErrCollection2String(pio_errmsg));
            RETURN;
        END IF;

        --ISS035-Group objects
        l_obj_type_table(801) := 'Empleados 1';
        l_obj_type_table(802) := 'Empleados 2';
        l_obj_type_table(803) := 'Obreros Alto Riesgo 1';
        l_obj_type_table(804) := 'Obreros Alto Riesgo 2';
        l_obj_type_table(805) := 'Obreros Bajo Riesgo 1';
        l_obj_type_table(806) := 'Obreros Bajo Riesgo 2';


        FOR l_obj_type_idx IN l_obj_type_table.first..l_obj_type_table.last
        LOOP

            --
            --Gather object (group) information
            --
            l_object_type := l_obj_type_idx;
            l_description := l_obj_type_table(l_obj_type_idx);
            --ISS041
            l_tariff_percent := CASE
                                    WHEN l_object_type = 801 THEN
                                        pi_fa_vley_row.empl1_rate
                                    WHEN l_object_type = 802 THEN
                                        pi_fa_vley_row.empl2_rate
                                    WHEN l_object_type = 803 THEN
                                        pi_fa_vley_row.high_risk1_rate
                                    WHEN l_object_type = 804 THEN
                                        pi_fa_vley_row.high_risk2_rate
                                    WHEN l_object_type = 805 THEN
                                        pi_fa_vley_row.low_risk1_rate
                                    WHEN l_object_type = 806 THEN
                                        pi_fa_vley_row.low_risk2_rate
                                    ELSE
                                        NULL
                                    END;

            putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'object|tariff_percent: ' || l_object_type ||'-'|| l_description ||'|'||l_tariff_percent);


            --ISS041
            --Exclude group without premium rate
            --
            IF l_tariff_percent IS NULL THEN
                CONTINUE;
            END IF;
            --
            --
            --



--            BEGIN
--                IF l_parent_obj_type IS NOT NULL THEN
--                    l_parent_obj_type_aux := l_parent_obj_type;
--                END IF;
--
--                SELECT
--                    gr.ref_group_object_type
--                INTO l_parent_obj_type
--                FROM
--                    insis_cust.cfglpv_groups_allowance gr
--                WHERE
--                        gr.insr_type = pi_fa_vley_row.insis_product_code
--                    AND object_type = l_object_type;
--
--                IF l_parent_obj_type = l_parent_obj_type_aux THEN
--                    count_parent_obj_type := 1;
--                END IF;
--            EXCEPTION
--                WHEN OTHERS THEN
--                    putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,
--                            'updatepengagementbilling.err:'||sqlerrm);
--                    return;
--            END;
--
--            IF l_parent_obj_type IS NOT NULL THEN
--                IF count_parent_obj_type = 0 THEN

                    --???? todo: validar si mentener
--                    insis_gen_v10.srv_object_data.gogroupinsrecord := null;

                    --================================================================================================
                    --PREPARE INFORMATION FOR INS_GROUP_INS
                    --================================================================================================
                    putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'--INS_GROUP_INS' );

                    --set to null to get a new value each time
                    insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'OBJECT_ID', insis_sys_v10.srv_context.integers_format, NULL);

--                    putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'INS_GROUP_INS.pre' );
--                    putlogcontext(pi_fa_vley_row.control_id, l_outcontext);

                    insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'OBJECT_TYPE', insis_sys_v10.srv_context.integers_format, l_object_type);
                    insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'MAIN_OBJECT_ID', insis_sys_v10.srv_context.integers_format, NULL);
                    --ISS038-Assign plan
                    insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'OGPP1', pi_fa_vley_row.PLAN);
                    insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'DESCRIPTION', l_description);


                    --================================================================================================
                    -- INS_GROUP_INS
                    --================================================================================================
                    insis_sys_v10.srv_events.sysevent('INS_GROUP_INS', l_outcontext, l_outcontext, pio_errmsg);

--                    putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'INS_GROUP_INS.post' );
--                    putlogcontext(pi_fa_vley_row.control_id, l_outcontext);


                    IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
                        putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,
                              'INS_GROUP_INS.err:'||srv_error.ErrCollection2String(pio_errmsg));
                        RETURN;
                    END IF;


                    --================================================================================================
                    --PREPARE INFORMATION FOR INSERT_INSURED_OBJECT EVENT
                    --================================================================================================

                    putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'--INSERT_INSURED_OBJECT' );
                    insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_ID', insis_sys_v10.srv_context.integers_format, l_master_policy_id);
                    insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ANNEX_ID', insis_sys_v10.srv_context.integers_format, insis_gen_v10.gvar_pas.def_annx_id);
                    insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'INSR_TYPE', insis_sys_v10.srv_context.integers_format, pi_fa_vley_row.insr_type);
                    insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'OBJECT_ID', insis_sys_v10.srv_context.integers_format, insis_gen_v10.srv_object_data.gogroupinsrecord.object_id);

                    --================================================================================================
                    -- INSERT_INSURED_OBJECT
                    -- Output parameter : INSURED_OBJ_ID
                    --================================================================================================

                    insis_sys_v10.srv_events.sysevent('INSERT_INSURED_OBJECT', l_outcontext, l_outcontext, pio_errmsg);
                    IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
                        putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,
                              'INSERT_INSURED_OBJECT.err:'||srv_error.ErrCollection2String(pio_errmsg));
                        RETURN;
                    END IF;

                    insis_sys_v10.srv_context.getcontextattrnumber(l_outcontext, 'INSURED_OBJ_ID', l_parent_ins_obj_id);

                    putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'l_parent_ins_obj_id: '||l_parent_ins_obj_id);
--                END IF;
--
                BEGIN
                    --ISS053--Insured object Currency
                    UPDATE insis_gen_v10.insured_object io
                    SET io.av_currency = pi_fa_vley_row.currency,
                        io.iv_currency = pi_fa_vley_row.currency
                    WHERE io.policy_id = l_master_policy_id
                    AND io.object_type = l_object_type
                    RETURNING io.insured_obj_id INTO l_ins_obj_id;

                EXCEPTION
                    WHEN OTHERS THEN
                        putlog(pi_fa_vley_row.control_id, pi_fa_vley_row.stag_id,
                              'INSERT_INSURED_OBJECT.err:'||SQLERRM);
                        srv_error_set('select insured_obj_id', 'WFE-08006', SQLERRM, pio_errmsg);
                        RETURN;
                END;
                putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'l_ins_obj_id: '||l_ins_obj_id);

                --================================================================================================
                --PREPARE INFORMATION FOR FILL_COVERS_FOR_SELECT EVENT
                --================================================================================================
                putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'--FILL_COVERS_FOR_SELECT' );

                insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'INSURED_OBJ_ID', insis_sys_v10.srv_context.integers_format, l_ins_obj_id);
                insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_ID', insis_sys_v10.srv_context.integers_format, l_master_policy_id);
                insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ANNEX_ID', insis_sys_v10.srv_context.integers_format, insis_gen_v10.gvar_pas.def_annx_id);

                --================================================================================================
                -- FILL_COVERS_FOR_SELECT
                -- Output parameter : TRUE or FALSE
                --================================================================================================

                insis_sys_v10.srv_events.sysevent('FILL_COVERS_FOR_SELECT', l_outcontext, l_outcontext, pio_errmsg);
                insis_sys_v10.srv_context.getcontextattrchar(l_outcontext, 'PROCEDURE_RESULT', l_procedure_result);
                IF upper(l_procedure_result) = 'FALSE' THEN
                        putlog(pi_fa_vley_row.control_id, pi_fa_vley_row.stag_id,
                              'FILL_COVERS_FOR_SELECT.err:'||srv_error.ErrCollection2String(pio_errmsg));
                    RETURN;
                END IF;

                --
                -- SELECT ALL COVERS
                --
                putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'--SELECTING_ALL_COVERS' );

                --ISS044--
                --For plan 6-Tailored Plan, choose if all covers are shown, or only legal ones.
                --NOTE: For Plan others than 6, all covers are being included by default according product configuration
                IF pi_fa_vley_row.PLAN = 6 AND
                   pi_fa_vley_row.legal_cov_flag = 'N'  --"only legal cover?"
                THEN
                    --todo : use standar objects
                    --todo : usar rutina generica. recbir valores, y nombre cobertura: if valores no nulos update ... where cover_type = ...
                    --ISS052--Only select covers with numer of salaries, or max sum insur.
                    UPDATE insis_gen_v10.gen_covers_select gcs
                    SET
                        apply_cover = 1
                    WHERE
                        gcs.policy_id = l_master_policy_id
                    AND (
                          ( gcs.cover_type = 'FESL' AND (nvl(pi_fa_vley_row.FE_NUM_SAL,0)>0 OR nvl(pi_fa_vley_row.FE_MAX_SI,0) > 0) )
                       OR
                          ( gcs.cover_type = 'DEBTPAYMSL' AND (nvl(pi_fa_vley_row.DESG_NUM_SAL,0) > 0 OR nvl(pi_fa_vley_row.DESG_MAX_SI,0) > 0) )
                       OR
                          ( gcs.cover_type = 'HOMELESSSL' AND (nvl(pi_fa_vley_row.HOMELESS_NUM_SAL,0)>0 OR nvl(pi_fa_vley_row.HOMELESS_MAX_SI,0) > 0) )
                       OR
                          ( gcs.cover_type = 'ANTTERMSL' AND (nvl(pi_fa_vley_row.ANTTERMILL_NUM_SAL, 0) > 0 OR nvl(pi_fa_vley_row.ANTTERMILL_MAX_SI,0) > 0) )
                       OR
                          ( gcs.cover_type = 'DEATHCANC' AND (nvl(pi_fa_vley_row.CANCER_DEATH_NUM_SAL,0) > 0 OR nvl(pi_fa_vley_row.CANCER_DEATH_MAX_SI, 0) > 0) )
                       OR
                          ( gcs.cover_type = 'CANCERSL' AND (nvl(pi_fa_vley_row.CANCER_NUM_SAL, 0) > 0 OR nvl(pi_fa_vley_row.CANCER_MAX_SI, 0) > 0) )
                       OR
                          ( gcs.cover_type = 'CID_MI' AND (nvl(pi_fa_vley_row.CRITMYO_NUM_SAL, 0) >0 OR nvl(pi_fa_vley_row.CRITMYO_MAX_SI, 0) >0) )
                       OR
                          ( gcs.cover_type = 'CID_STROKE' AND (nvl(pi_fa_vley_row.CISTROKE_NUM_SAL,0) > 0 OR nvl(pi_fa_vley_row.CISTROKE_MAX_SI, 0) >0) )
                       OR
                          ( gcs.cover_type = 'CID_CRF' AND (nvl(pi_fa_vley_row.CICRF_NUM_SAL,0) > 0 OR  nvl(pi_fa_vley_row.CICRF_MAX_SI, 0) >0) )
                       OR
                          ( gcs.cover_type = 'CID_MULSC' AND (nvl(pi_fa_vley_row.CIMULTSCL_NUM_SAL,0) > 0 OR nvl(pi_fa_vley_row.CIMULTSCL_MAX_SI, 0) >0) )
                       OR
                          ( gcs.cover_type = 'CID_COMA' AND (nvl(pi_fa_vley_row.CICOMA_NUM_SAL,0) > 0 OR nvl(pi_fa_vley_row.CICOMA_MAX_SI, 0) >0) )
                       OR
                          ( gcs.cover_type = 'CID_BYPS' AND (nvl(pi_fa_vley_row.CIBYPASS_NUM_SAL,0) > 0 OR nvl(pi_fa_vley_row.CIBYPASS_MAX_SI, 0) >0) )
                       OR
                          ( gcs.cover_type = 'CID_ORGTR' AND (nvl(pi_fa_vley_row.CRITILL_NUM_SAL,0) > 0 OR nvl(pi_fa_vley_row.CRITILL_MAX_SI, 0) >0) )
                       OR
                          ( gcs.cover_type = 'BLINDACC' AND (nvl(pi_fa_vley_row.BLINDNESS_NUM_SAL,0) > 0 OR nvl(pi_fa_vley_row.BLINDNESS_MAX_SI, 0) >0) )
                       OR
                          ( gcs.cover_type = 'CRITBURACC' AND (nvl(pi_fa_vley_row.CRITBURN_NUM_SAL,0) > 0 OR nvl(pi_fa_vley_row.CRITBURN_MAX_SI, 0) >0) )
                       OR
                          ( gcs.cover_type = 'CHILDBRNDT' AND (nvl(pi_fa_vley_row.POSTHUM_CHILD_NUM_SAL,0) > 0 OR nvl(pi_fa_vley_row.POSTHUM_CHILD_MAX_SI, 0) >0) )
                       OR
                          ( gcs.cover_type = 'TDEAFACCS' AND (nvl(pi_fa_vley_row.DEAFNESS_NUM_SAL,0) > 0 OR nvl(pi_fa_vley_row.DEAFNESS_MAX_SI, 0) >0) )
                       OR
                          ( gcs.cover_type = 'FAMALLOW' AND (nvl(pi_fa_vley_row.FAM_SAL_PERC,0) > 0 OR nvl(pi_fa_vley_row.FAM_NUM_SAL,0)>0 OR nvl(pi_fa_vley_row.FAM_MAX_SI, 0) >0) )
                       OR
                          ( gcs.cover_type = 'REPREMSL' AND (nvl(pi_fa_vley_row.REPREM_NUM_SAL,0) > 0 OR nvl(pi_fa_vley_row.REPREM_MAX_SI, 0) > 0) )
                       OR
                          ( gcs.cover_type = 'INABILWORK' AND (nvl(pi_fa_vley_row.INABWORK_NUM_SAL,0) > 0 OR (nvl(pi_fa_vley_row.INABWORK__MAX_SI,0) > 0) ))
                       OR
                          ( gcs.cover_type = 'TRANSFERSL' AND (nvl(pi_fa_vley_row.TRANSFER_NUM_SAL,0) > 0 OR nvl(pi_fa_vley_row.TRANSFER_MAX_SI,0) >0) )
                        )
                        ;
                END IF;

                --
                --PREPARE INFORMATION FOR ATTACH_SELECTED_COVERS EVENT
                --
                putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'--ATTACH_SELECTED_COVERS' );

                insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'INSURED_OBJ_ID', insis_sys_v10.srv_context.integers_format, l_ins_obj_id);
                insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_ID', insis_sys_v10.srv_context.integers_format, l_master_policy_id);
                insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ANNEX_ID', insis_sys_v10.srv_context.integers_format, insis_gen_v10.gvar_pas.def_annx_id);

                --================================================================================================
                -- ATTACH_SELECTED_COVERS
                -- Output parameter : TRUE or FALSE
                --================================================================================================

                insis_sys_v10.srv_events.sysevent('ATTACH_SELECTED_COVERS', l_outcontext, l_outcontext, pio_errmsg);
                insis_sys_v10.srv_context.getcontextattrchar(l_outcontext, 'PROCEDURE_RESULT', l_procedure_result);
                IF upper(l_procedure_result) = 'FALSE' THEN
                    putlog(pi_fa_vley_row.control_id, pi_fa_vley_row.stag_id,
                          'ATTACH_SELECTED_COVERS.err:'||srv_error.ErrCollection2String(pio_errmsg));
                    RETURN;
                END IF;

                putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'UPDATE tariff_percent' );

                --ISS053--Insured object Currency, all covers
                --ISS056--Set "Percent (Annual)", only main cover

                --todo : use standard objects
                UPDATE insis_gen_v10.gen_risk_covered
                SET
                    currency              = pi_fa_vley_row.currency,
                    tariff_percent        = CASE WHEN cover_type = 'NATDEATHSL' THEN l_tariff_percent ELSE tariff_percent END,
                    --Percent (Annual)
                    manual_prem_dimension = CASE WHEN cover_type = 'NATDEATHSL' THEN gvar_pas.PREM_DIM_P ELSE manual_prem_dimension END
                WHERE
                        insured_obj_id = l_ins_obj_id
                    ;

--                END IF;
--
--            END IF;
--
        END LOOP;
--
        --================================================================================================
        --PREPARE INFORMATION FOR FILL_POLICY_CONDITIONS EVENT
        --================================================================================================
--        putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'--FILL_POLICY_CONDITIONS' );

        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ANNEX_ID', insis_sys_v10.srv_context.integers_format, insis_gen_v10.gvar_pas.def_annx_id);
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_ID', insis_sys_v10.srv_context.integers_format, l_master_policy_id);
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'INSR_TYPE', insis_sys_v10.srv_context.integers_format, pi_fa_vley_row.insr_type);

        --================================================================================================
        -- FILL_POLICY_CONDITIONS
        -- Output parameter : TRUE or FALSE
        --================================================================================================

        insis_sys_v10.srv_events.sysevent('FILL_POLICY_CONDITIONS', l_outcontext, l_outcontext, pio_errmsg);
        insis_sys_v10.srv_context.getcontextattrchar(l_outcontext, 'PROCEDURE_RESULT', l_procedure_result);
        IF upper(l_procedure_result) = 'FALSE' THEN
            putlog(pi_fa_vley_row.control_id, pi_fa_vley_row.stag_id,
                  'FILL_POLICY_CONDITIONS.err:'||srv_error.ErrCollection2String(pio_errmsg));
            RETURN;
        END IF;

        --
        -- policy_condition update : AS_IS
        --
--        putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'--UPDATING POLICY_CONDITION:AS_IS' );
--todo: add as validation
--        IF pi_fa_vley_row.insr_type = 2009 AND pi_fa_vley_row.as_is <> '0'
--        THEN
            update_conditions('2009', pi_fa_vley_row.as_is, NULL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);

--        END IF;

        --1:no, 2:yes
        update_conditions('CONSORCIO', yn_to_num(pi_fa_vley_row.consortium_flag,2,1), NULL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        --1:no, 2:yes
        update_conditions('LICITACION_TENDER', yn_to_num(pi_fa_vley_row.tender_flag,2,1), NULL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        --1:yes, 2:no
--        update_conditions('LEGAL_WAGE_LIMIT', yn_to_num(pi_fa_vley_row.legal_limit_flag,1,2), NULL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        update_conditions('LEGAL_WAGE_LIMIT', yn_to_num(pi_fa_vley_row.legal_limit_clause_flag,1,2), NULL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        --1:yes, 2:no. Note: 'UW_WAGE_LIMIT' has opposite value than 'LEGAL_WAGE_LIMIT', that is the reason to change Y-N param values
--        update_conditions('UW_WAGE_LIMIT', yn_to_num(pi_fa_vley_row.legal_limit_flag,2,1), NULL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        update_conditions('UW_WAGE_LIMIT', yn_to_num(pi_fa_vley_row.no_salary_limit_flag,1,2), NULL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        --1:yes, 2:no
        update_conditions('NO_NOMINATIVA', yn_to_num(pi_fa_vley_row.unid_policy_flag,2,1), NULL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);

        update_conditions('FACTURA_POR', pi_fa_vley_row.billing_by, NULL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        update_conditions('TIPO_FACTURATION', pi_fa_vley_row.billing_type, NULL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        update_conditions('TYPE_CALC', pi_fa_vley_row.prem_cal_period, NULL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        --ISS036-Add BROKER/BROKERVAT agent
        --ISS045-Commision value already is received as percent. No need to x100
        update_conditions('BROKER_1_COMM', 1, pi_fa_vley_row.brok_comm_perc, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);

        --ISS054: Salary limits
        IF pi_fa_vley_row.natdeath_sal IS NOT NULL THEN
            update_conditions('NUM_MNT_SAL_NATD', 1, pi_fa_vley_row.natdeath_sal, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        END IF;
        IF pi_fa_vley_row.accdeath_sal IS NOT NULL THEN
            update_conditions('NUM_MNT_SAL_ACCD', 1, pi_fa_vley_row.accdeath_sal, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        END IF;
        IF pi_fa_vley_row.itpa_sal IS NOT NULL THEN
            update_conditions('NUM_MNT_SAL_TBDA', 1, pi_fa_vley_row.itpa_sal, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        END IF;

--
--        --================================================================================================
--        -- CUST_POLICY_DEFAULT_PARAMS
--        -- Output parameter : POLICY_ID
--        --================================================================================================
--        putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'--CUST_POLICY_DEFAULT_PARAMS' );
--
--        insis_sys_v10.srv_events.sysevent('CUST_POLICY_DEFAULT_PARAMS', l_outcontext, l_outcontext, pio_errmsg);
--
--
        --================================================================================================
        --PREPARE INFORMATION FOR LOAD_QUEST EVENT
        --================================================================================================
--        putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'--LOAD_QUEST' );

        insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'REFERENCE_TYPE', 'POLICY');
        insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'TO_LOAD', 'Y');
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_ID', insis_sys_v10.srv_context.integers_format, l_master_policy_id);
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ANNEX_ID', insis_sys_v10.srv_context.integers_format, insis_gen_v10.gvar_pas.def_annx_id);
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'PHOLDER_ID', insis_sys_v10.srv_context.integers_format, insis_gen_v10.srv_policy_data.gpolicyrecord.client_id);

        --================================================================================================
        -- LOAD_QUEST
        -- Output parameter : TRUE or FALSE
        --================================================================================================


        insis_sys_v10.srv_events.sysevent('LOAD_QUEST', l_outcontext, l_outcontext, pio_errmsg);
        insis_sys_v10.srv_context.getcontextattrchar(l_outcontext, 'PROCEDURE_RESULT', l_procedure_result);
        IF upper(l_procedure_result) = 'FALSE' THEN
            putlog(pi_fa_vley_row.control_id, pi_fa_vley_row.stag_id,
                  'LOAD_QUEST.err:'||srv_error.ErrCollection2String(pio_errmsg));

            RETURN;
        END IF;

        --ISS055-Fix EPOLR
        l_quest_answer := CASE WHEN pi_fa_vley_row.epolicy_flag = 'Y' THEN
                                    3
                          ELSE
                                    4
                          END;
        update_quest('POL', 'EPOLR', l_quest_answer, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);

        --ISS046
        IF pi_fa_vley_row.PLAN = 6
        THEN
            update_quest('POL', '2009.01', pi_fa_vley_row.FE_NUM_SAL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.02', pi_fa_vley_row.FE_MAX_SI, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.03', pi_fa_vley_row.DESG_NUM_SAL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.04', pi_fa_vley_row.DESG_MAX_SI, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.05', pi_fa_vley_row.HOMELESS_NUM_SAL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.06', pi_fa_vley_row.HOMELESS_MAX_SI, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.07', pi_fa_vley_row.ANTTERMILL_NUM_SAL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.08', pi_fa_vley_row.ANTTERMILL_MAX_SI, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.11', pi_fa_vley_row.CANCER_DEATH_NUM_SAL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.12', pi_fa_vley_row.CANCER_DEATH_MAX_SI, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.09', pi_fa_vley_row.CANCER_NUM_SAL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.10', pi_fa_vley_row.CANCER_MAX_SI, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.13', pi_fa_vley_row.CRITMYO_NUM_SAL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.14', pi_fa_vley_row.CRITMYO_MAX_SI, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.15', pi_fa_vley_row.CISTROKE_NUM_SAL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.16', pi_fa_vley_row.CISTROKE_MAX_SI, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.17', pi_fa_vley_row.CICRF_NUM_SAL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.18', pi_fa_vley_row.CICRF_MAX_SI, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.19', pi_fa_vley_row.CIMULTSCL_NUM_SAL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.20', pi_fa_vley_row.CIMULTSCL_MAX_SI, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.21', pi_fa_vley_row.CICOMA_NUM_SAL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.22', pi_fa_vley_row.CICOMA_MAX_SI, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.23', pi_fa_vley_row.CIBYPASS_NUM_SAL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.24', pi_fa_vley_row.CIBYPASS_MAX_SI, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.25', pi_fa_vley_row.CRITILL_NUM_SAL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.26', pi_fa_vley_row.CRITILL_MAX_SI, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.27', pi_fa_vley_row.BLINDNESS_NUM_SAL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.28', pi_fa_vley_row.BLINDNESS_MAX_SI, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.29', pi_fa_vley_row.CRITBURN_NUM_SAL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.30', pi_fa_vley_row.CRITBURN_MAX_SI, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.31', pi_fa_vley_row.POSTHUM_CHILD_NUM_SAL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.32', pi_fa_vley_row.POSTHUM_CHILD_MAX_SI, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.33', pi_fa_vley_row.DEAFNESS_NUM_SAL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.34', pi_fa_vley_row.DEAFNESS_MAX_SI, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.35', pi_fa_vley_row.FAM_SAL_PERC, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.36', pi_fa_vley_row.FAM_NUM_SAL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.37', pi_fa_vley_row.FAM_MAX_SI, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.38', pi_fa_vley_row.REPREM_NUM_SAL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.39', pi_fa_vley_row.REPREM_MAX_SI, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.40', pi_fa_vley_row.INABWORK_NUM_SAL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.41', pi_fa_vley_row.INABWORK__MAX_SI, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.42', pi_fa_vley_row.TRANSFER_NUM_SAL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
            update_quest('POL', '2009.43', pi_fa_vley_row.TRANSFER_MAX_SI, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
        END IF;


        --ISS033-Load endorsements
        --================================================================================================
        --PREPARE INFORMATION FOR INSERT_ENDORSEMENT EVENT
        --================================================================================================
--        putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'--INSERT_ENDORSEMENT' );
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_ID', insis_sys_v10.srv_context.integers_format, l_master_policy_id);
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ANNEX_ID', insis_sys_v10.srv_context.integers_format, insis_gen_v10.gvar_pas.def_annx_id);

        --================================================================================================
        -- INSERT_ENDORSEMENT
        -- Output parameter :
        --================================================================================================

        insis_sys_v10.srv_events.sysevent('INSERT_ENDORSEMENT', l_outcontext, l_outcontext, pio_errmsg);
        insis_sys_v10.srv_context.getcontextattrchar(l_outcontext, 'PROCEDURE_RESULT', l_procedure_result);

        IF upper(l_procedure_result) = 'FALSE' THEN
            putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'INSERT_ENDORSEMENT.err' );
            RETURN;
        END IF;

        --Delete endorsement according flags
        --todo:usar objetos y constantes, o rutina generica
        IF pi_fa_vley_row.legal_limit_clause_flag = 'N' THEN
            DELETE insis_gen_v10.policy_endorsements
            WHERE
                policy_id = l_master_policy_id
            AND endorsement_code IN('608');
        END IF;

        IF pi_fa_vley_row.no_salary_limit_flag = 'N' THEN
            DELETE insis_gen_v10.policy_endorsements
            WHERE
                policy_id = l_master_policy_id
            AND endorsement_code IN('601');
        END IF;

        IF pi_fa_vley_row.indem_pay_clause_flag = 'N' THEN
            DELETE insis_gen_v10.policy_endorsements
            WHERE
                policy_id = l_master_policy_id
            AND endorsement_code IN('602');
        END IF;

        IF pi_fa_vley_row.claim_pay_clause_flag = 'N' THEN
            DELETE insis_gen_v10.policy_endorsements
            WHERE
                policy_id = l_master_policy_id
            AND endorsement_code IN('603');
        END IF;

        IF pi_fa_vley_row.currency_clause_flag = 'N' THEN
            DELETE insis_gen_v10.policy_endorsements
            WHERE
                policy_id = l_master_policy_id
            AND endorsement_code IN('605');
        END IF;

        IF pi_fa_vley_row.WAITING_CLAUSE_FLAG = 'N' THEN
            DELETE insis_gen_v10.policy_endorsements
            WHERE
                policy_id = l_master_policy_id
            AND endorsement_code IN('606');
        END IF;

        --todo:opcional. debiera crearse si flag esta activo (no carga automatico)
        IF pi_fa_vley_row.special_clause_text IS NOT NULL THEN
            UPDATE insis_gen_v10.policy_endorsements
            SET
                text = pi_fa_vley_row.special_clause_text
            WHERE
                    policy_id = l_master_policy_id
                AND endorsement_code = '604' --special clause
                AND cover_type IS NULL;

        END IF;

--
        --================================================================================================
        --PREPARE INFORMATION FOR CALC_PREM EVENT
        --================================================================================================
--        putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'--CALC_PREM' );
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_ID', insis_sys_v10.srv_context.integers_format, l_master_policy_id);
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ANNEX_ID', insis_sys_v10.srv_context.integers_format, insis_gen_v10.gvar_pas.def_annx_id);

        insis_sys_v10.srv_events.sysevent('CALC_PREM', l_outcontext, l_outcontext, pio_errmsg);
        IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
            putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'CALC_PREM.err|'||srv_error.ErrCollection2String(pio_errmsg) );
            RETURN;
        END IF;

        --================================================================================================
        -- APPL_CONF
        -- Output parameter :
        --================================================================================================
--        putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'--APPL_CONF' );

        insis_sys_v10.srv_events.sysevent('APPL_CONF', l_outcontext, l_outcontext, pio_errmsg);
        IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
            putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'APPL_CONF.err|'||srv_error.ErrCollection2String(pio_errmsg) );
            RETURN;
        END IF;


        --ISS037-Defines final policy status
        IF pi_fa_vley_row.policy_state = gvar_pas.PSM_OPEN
        THEN
            --================================================================================================
            -- APPL_CONV
            -- Output parameter :
            --================================================================================================
            putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'--APPL_CONV' );
            insis_sys_v10.srv_events.sysevent('APPL_CONV', l_outcontext, l_outcontext, pio_errmsg);
            IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
                putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'APPL_CONV.err|'||srv_error.ErrCollection2String(pio_errmsg) );
                RETURN;
            END IF;
        END IF;

--        putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'Update policy policy_name date_covered' );

        UPDATE insis_gen_v10.POLICY
        SET
            policy_name  = pi_fa_vley_row.master_policy_no,
            date_covered = l_date_covered
        WHERE
            policy_id = l_master_policy_id;


--        UPDATE cust_migration.fa_migr_vlt_mp_pol
--        SET
--            att_new_policy_id = l_master_policy_id
--        WHERE
--                control_id = pi_fa_vley_row.control_id
--            AND stag_id = pi_fa_vley_row.stag_id;

        putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'vley_record_proc|end');

    EXCEPTION
        WHEN OTHERS THEN
            putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'vley_record_proc|end_err' || SQLERRM);
            srv_error_set('vley_record_proc', 'SYSERROR', SQLERRM, pio_errmsg);
    END vley_record_proc;

    ---------------------------------------------------------------------------------
    -- Name: fa_cust_migr_vlt_mp.get_last_record_for_report
    ---------------------------------------------------------------------------------
    -- Purpose: get last process id to generate a report
    -- Type: PROCEDURE
    -- Status: ACTIVE
    -- Versioning:
    --     LPV-FRAMEND0     2020-04-14      creation
    ---------------------------------------------------------------------------------
    PROCEDURE get_last_record_for_report (
        po_poller_id     OUT  NUMBER,
        po_file_name     OUT  VARCHAR2,
        po_success_flag  OUT  INTEGER
    )
    IS
    BEGIN
        l_log_proc := '0';
        putlog(0, 0, 'get_last_record_for_report|start| ' || po_poller_id);

        po_success_flag := 1;
        SELECT
            sys_poller_process_ctrl_id,
            substr(file_name, 0,
                    instr(file_name, '.') - 1) || '_' ||
                    sys_poller_process_ctrl_id || '-' ||
                    to_char(date_init, 'DDMMYYYY') || '_' ||
                    to_char(date_init, 'HH24MISS') || '_' || '.xlsx'
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
                    (   --recover oldest process record ---that has data processed (status 2 or 3))
                        SELECT
                            control_id
                        FROM
                            cust_migration.fa_migr_vley_err ctrl
                        WHERE
                            stag_id    = 0
                            AND errseq    = 0
                            AND errtype = 'REP' --record ready for report
--                        and exists (select 1
--                                      from cust_migration.fa_migr_vlt_mp_pol stg
--                                     where stg.control_id = ctrl.batch_id
--                                     and stg.att_status_rec in (2,3))
                        ORDER BY
                            control_id ASC
                    )
                WHERE
                    ROWNUM = 1
            );

        putlog(po_poller_id, 0, 'get_last_record_for_report|end| ' || po_poller_id);

    EXCEPTION
        WHEN OTHERS THEN
            po_success_flag := 0;
            putlog(0, 0, 'get_last_record_for_reportget_last_record_for_report|end_err| ' || SQLERRM);
    END get_last_record_for_report;

    ---------------------------------------------------------------------------------
    -- Name: fa_cust_migr_vlt_mp.upd_last_record_report
    ---------------------------------------------------------------------------------
    -- Purpose: Updates last process record after report was generated
    -- Type: PROCEDURE
    -- Status: ACTIVE
    -- Versioning:
    --     LPV-FRAMEND0     2020-04-14      creation
    ---------------------------------------------------------------------------------
    PROCEDURE upd_last_record_report (
        pi_control_id_rep       IN  NUMBER,
        pi_file_id              IN  NUMBER,
        pi_control_id_proc      IN  NUMBER
    ) IS
    BEGIN
        l_log_proc := pi_control_id_rep;
        putlog(pi_control_id_rep, 0, 'upd_last_record_report|start|control_id_rep,pi_control_id_proc: ' || pi_control_id_rep||','||pi_control_id_proc);

        UPDATE insis_cust_lpv.sys_poller_process_ctrl
        SET
            file_id = pi_file_id
        WHERE
            sys_poller_process_ctrl_id = pi_control_id_rep;

        insis_cust_lpv.sys_schema_utils.update_poller_process_status(pi_control_id_rep, 'SUCCESS');


        UPDATE cust_migration.fa_migr_vley_err
        SET
            errtype = 'GEN',
            errmess = '--Report generated--'
        WHERE
                control_id = pi_control_id_proc
            AND stag_id = 0
            AND errseq = 0
            AND errtype = 'REP';

        COMMIT;

        putlog(pi_control_id_rep,0,'upd_last_record_report|end');

    EXCEPTION
        WHEN OTHERS THEN
            putlog(pi_control_id_rep, 0, 'upd_last_record_report|end_err|' || SQLERRM);
    END upd_last_record_report;


    PROCEDURE ins_error_stg (
        pi_sys_ctrl_id  IN      fa_migr_vley_err.control_id%TYPE,
        pi_stg_id       IN      fa_migr_vley_err.stag_id%TYPE,
        pi_errseq       IN      fa_migr_vley_err.errseq%TYPE,
        pi_errtype      IN      fa_migr_vley_err.errtype%TYPE,
        pi_errcode      IN      fa_migr_vley_err.errcode%TYPE,
        pi_errmess      IN      fa_migr_vley_err.errmess%TYPE,
        pio_errmsg      IN OUT  srverr
    ) IS
        PRAGMA autonomous_transaction;
        l_errmsg                   srverrmsg;
    BEGIN


        INSERT INTO fa_migr_vley_err(CONTROL_ID,STAG_ID,ERRSEQ,ERRTYPE,ERRCODE,ERRMESS)
        VALUES (pi_sys_ctrl_id, pi_stg_id, pi_errseq, pi_errtype, pi_errcode, pi_errmess);

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            srv_error.setsyserrormsg( l_errmsg, 'insert_error_stg', SQLERRM );
            srv_error.seterrormsg( l_errmsg, pio_errmsg );
    END ins_error_stg;

--    PROCEDURE insert_error_stg2 (
--        pi_sys_ctrl_id  IN      NUMBER,
--        pi_stg_id       IN      NUMBER,
--        pi_rowseq         IN      VARCHAR2,
--        pi_error_code   IN      NUMBER,
--        pi_fn_name      IN      VARCHAR2,
--        pi_sqlerrm      IN      VARCHAR2,
--        pio_errmsg      IN OUT  srverr
--    ) IS
--        PRAGMA autonomous_transaction;
--        l_errmsg                   srverrmsg;
--    BEGIN
--        insis_sys_v10.srv_error.setsyserrormsg(l_errmsg, pi_fn_name, pi_sqlerrm);
--        insis_sys_v10.srv_error.seterrormsg(l_errmsg, pio_errmsg);
--
--        insert into fa_cust_migr_vlt_mp_error_log(sys_poller_process_ctrl_id, stag_id, rowseq, error_code, error_message)
--        values (pi_sys_ctrl_id, pi_stg_id, pi_rowseq, pi_error_code, l_errmsg.errmessage);
--
--        COMMIT;
--
--    EXCEPTION
--        WHEN OTHERS THEN
--            ROLLBACK;
--    END;
--
--    PROCEDURE insert_error_stg3 (
--        pi_sys_ctrl_id  IN      NUMBER,
--        pi_stg_id       IN      NUMBER,
--        pi_rowseq         IN      VARCHAR2,
--        pi_error_code   IN      NUMBER,
--        pi_fn_name      IN      VARCHAR2,
--        pio_errmsg      IN OUT  srverr
--    ) IS
--        PRAGMA autonomous_transaction;
--        l_errmsg                   srverrmsg;
--    BEGIN
--
--
--        insert into fa_cust_migr_vlt_mp_error_log(sys_poller_process_ctrl_id, stag_id, rowseq, error_code, error_message)
--        values (pi_sys_ctrl_id, pi_stg_id, pi_rowseq, pi_error_code,
--                pi_fn_name || '-' || insis_sys_v10.srv_error.errcollection2string(pio_errmsg) || '(' || pio_errmsg(1).errfn || ')');
--
--        COMMIT;
--    EXCEPTION
--        WHEN OTHERS THEN
--            ROLLBACK;
--    END;
--
    PROCEDURE reverse_proc (
        pi_sys_ctrl_id  IN  NUMBER,
        pi_file_id      IN  NUMBER,
        pi_file_name    IN  VARCHAR
    ) IS
--

        v_lpv_migr_spf_det  cust_migration.fa_migr_vlt_mp_pol%ROWTYPE;
        v_code              VARCHAR(4000);
        pio_err             srverr;
        v_errm              VARCHAR(4000);
        v_file_id           NUMBER;
        v_result            BOOLEAN := FALSE;--
    BEGIN
    --starting point for log sequence
    --sample: 1200000000000 + 34223 => 1203422300000
        l_log_seq  := l_log_seq_ini + ( pi_sys_ctrl_id * 1000000 );
        l_log_proc := pi_sys_ctrl_id;


        DELETE sta_log
        WHERE table_name = LC_LOG_TABLE_NAME
        AND batch_id LIKE to_char(pi_sys_ctrl_id) || '%';

        DELETE cust_migration.fa_migr_vley_err
        WHERE control_id = pi_sys_ctrl_id;


        --se actualizan policy_no generados para que no se dupliquen
        UPDATE insis_gen_v10.POLICY P
            set P.policy_no = substr(P.policy_id, 1, 4)||substr(P.policy_id, 7, 6),
                P.policy_name = substr(P.policy_id, 1, 4)||substr(P.policy_id, 7, 6)
        WHERE P.policy_id IN (SELECT att_new_policy_id
                            FROM cust_migration.fa_migr_vlt_mp_pol
                            WHERE control_id = reverse_proc.pi_sys_ctrl_id);


        putlog(pi_sys_ctrl_id, 0, 'reverse_proc|start|params|' || pi_sys_ctrl_id || ',' || pi_file_id || ',' || pi_file_name);

        putlog(pi_sys_ctrl_id, 0, 'reverse_proc|log deleted');
        putlog(pi_sys_ctrl_id, 0, 'reverse_proc|updating att');


        UPDATE cust_migration.fa_migr_vlt_mp_pol d
            SET
                att_status_rec = LC_STAT_REC_LOAD,
                att_new_policy_id = NULL
            WHERE
                control_id = reverse_proc.pi_sys_ctrl_id;

        COMMIT;

        putlog(pi_sys_ctrl_id, 0, 'reverse_proc|end');

    EXCEPTION
        WHEN OTHERS THEN
            insis_cust_lpv.sys_schema_utils.log_poller_error_process(pi_file_id, pi_file_name, LC_POLLER_NAME, SQLERRM, 'Process_SPF_Data');
            insis_cust_lpv.sys_schema_utils.update_poller_process_status(pi_sys_ctrl_id, 'ERROR');
            putlog(pi_sys_ctrl_id, 0,'reverse_proc|end_error');
    END reverse_proc;

END fa_cust_migr_vlt_mp;