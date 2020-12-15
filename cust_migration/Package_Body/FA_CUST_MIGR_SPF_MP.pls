create or replace PACKAGE BODY fa_cust_migr_spf_mp AS
--
--------------------------------------------------------------------------------
-- Name: fa_cust_migr_spf_mp
--
-- Type: PACKAGE
--
-- Subtype: 
--
-- Status: ACTIVE
--
-- Versioning:
--     LPV-framend0         2020-07-29  creation
--     LPV-framend0         2020-09-01  ISS090-SPF-MP-Translate object names to spanish
--     LPV-framend0         2020-09-01  ISS098-SPF-MP-Fix La Positiva office 
--     LPV-framend0         2020-09-30  ISS098-SPF-MP-Fix La Positiva office for 1 digit codes. 
--     LPV-framend0         2020-10-19  ISS110-SPF-MP-Updates policy's insr_type description
--                                                    Fix staff_id removing last update      
--                                                    Includes INTAGCOLL agent
---------------------------------------------------------------------------------
--
-- Purpose: Process Poller for 2014-SPF Master Policies migration 
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

--
--------------------------------------------------------------------------------
-- Name: fa_cust_migr_spf_mp.putlog
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
        pi_control_id  IN      fa_migr_spf_err.control_id%TYPE,
        pi_stag_id     IN      fa_migr_spf_err.stag_id%TYPE,
        pi_errtype     IN      fa_migr_spf_err.err_type%TYPE,
        pi_errcode     IN      fa_migr_spf_err.err_code%TYPE,
        pi_errmess     IN      fa_migr_spf_err.err_mess%TYPE,
        pio_errmsg     IN OUT  srverr
    ) IS
        PRAGMA autonomous_transaction;
        l_errmsg srverrmsg;
    BEGIN
        l_errseq := l_errseq + 1;
        INSERT INTO fa_migr_spf_err (
            control_id,
            stag_id,
            err_seq,
            err_type,
            err_code,
            err_mess
        ) VALUES (
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
            putlog(pi_control_id, 'ins_error_stg.err|' || pi_errcode || '|' || sqlerrm);
            srv_error.setsyserrormsg(l_errmsg, 'insert_error_stg', sqlerrm);
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
            insis_sys_v10.srv_error.setsyserrormsg(l_errmsg, 'srv_error_set', pi_fn_name || '|' || sqlerrm);
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
        pi_control_id              fa_migr_spf_mp_pol.control_id%TYPE,
        pi_stag_id                 fa_migr_spf_mp_pol.stag_id%TYPE,
        pi_insr_type               fa_migr_spf_mp_pol.insr_type%TYPE,
        pi_holder_egn              fa_migr_spf_mp_pol.holder_egn%TYPE,
        pi_holder_name             fa_migr_spf_mp_pol.holder_name%TYPE,
        pi_policy_no               fa_migr_spf_mp_pol.policy_no%TYPE,
        pi_insr_begin              fa_migr_spf_mp_pol.insr_begin%TYPE,
        pi_insr_end                fa_migr_spf_mp_pol.insr_end%TYPE,
        pi_insr_duration           fa_migr_spf_mp_pol.insr_duration%TYPE,
        pi_policy_state_desc       fa_migr_spf_mp_pol.policy_state_desc%TYPE,
        pi_sales_channel_id        fa_migr_spf_mp_pol.sales_channel_id%TYPE,
        pi_sales_channel_name      fa_migr_spf_mp_pol.sales_channel_name%TYPE,
        pi_office_id               fa_migr_spf_mp_pol.office_id%TYPE,
        pi_office_name             fa_migr_spf_mp_pol.office_name%TYPE,
        pi_billing_type            fa_migr_spf_mp_pol.billing_type%TYPE,
        pi_internal_agent_id       fa_migr_spf_mp_pol.internal_agent_id%TYPE,
        pi_internal_agent_name     fa_migr_spf_mp_pol.internal_agent_name%TYPE,
        pi_payment_way_name        fa_migr_spf_mp_pol.payment_way_name%TYPE,
        pi_currency                fa_migr_spf_mp_pol.currency%TYPE,
        pi_as_is                   fa_migr_spf_mp_pol.as_is%TYPE,
        pi_as_is_name              fa_migr_spf_mp_pol.as_is_name%TYPE,
        pi_lpv_employee_flag       fa_migr_spf_mp_pol.lpv_employee_flag%TYPE,
        pi_insr_group              fa_migr_spf_mp_pol.insr_group%TYPE,
        pi_prov_valtype            fa_migr_spf_mp_pol.prov_valtype%TYPE,
        pi_prov_value              fa_migr_spf_mp_pol.prov_value%TYPE,
        pi_prov_flag               fa_migr_spf_mp_pol.prov_flag%TYPE,
        pi_sales_module_id         fa_migr_spf_mp_pol.sales_module_id%TYPE,
        pi_sales_module_name       fa_migr_spf_mp_pol.sales_module_name%TYPE,
        pi_sales_channel_spf_id    fa_migr_spf_mp_pol.sales_channel_spf_id%TYPE,
        pi_sales_channel_spf_name  fa_migr_spf_mp_pol.sales_channel_spf_name%TYPE
    ) IS

        cn_proc  VARCHAR2(100) := 'upload_row_pol:' || pi_control_id;
        v_code   VARCHAR2(4000);
        v_errm   VARCHAR2(4000);
        v_id     NUMBER;
    BEGIN
--        putlog(pi_control_id, cn_proc || '|start|params: ' || pi_policy_no);
        BEGIN
            SELECT /*+ INDEX_DESC(stg FA_MIGR_SPF_MP_POL_PK) */
                stag_id + 1
            INTO v_id
            FROM
                cust_migration.fa_migr_spf_mp_pol stg
            WHERE
                    control_id = pi_control_id
                AND ROWNUM = 1;

        EXCEPTION
            WHEN no_data_found THEN
                v_id := 1;
        END;

        INSERT INTO cust_migration.fa_migr_spf_mp_pol (
            control_id,
            stag_id,
            insr_type,
            holder_egn,
            holder_name,
            policy_no,
            insr_begin,
            insr_end,
            insr_duration,
            policy_state_desc,
            sales_channel_id,
            sales_channel_name,
            office_id,
            office_name,
            billing_type,
            internal_agent_id,
            internal_agent_name,
            payment_way_name,
            currency,
            as_is,
            as_is_name,
            lpv_employee_flag,
            insr_group,
            prov_valtype,
            prov_value,
            prov_flag,
            sales_module_id,
            sales_module_name,
            sales_channel_spf_id,
            sales_channel_spf_name,
            att_status_rec
        ) VALUES (
            pi_control_id,
            v_id,
            pi_insr_type,
            pi_holder_egn,
            pi_holder_name,
            pi_policy_no,
            pi_insr_begin,
            pi_insr_end,
            pi_insr_duration,
            pi_policy_state_desc,
            pi_sales_channel_id,
            pi_sales_channel_name,
            pi_office_id,
            pi_office_name,
            pi_billing_type,
            pi_internal_agent_id,
            pi_internal_agent_name,
            pi_payment_way_name,
            pi_currency,
            pi_as_is,
            pi_as_is_name,
            pi_lpv_employee_flag,
            pi_insr_group,
            pi_prov_valtype,
            pi_prov_value,
            pi_prov_flag,
            pi_sales_module_id,
            pi_sales_module_name,
            pi_sales_channel_spf_id,
            pi_sales_channel_spf_name,
            cn_stat_rec_load
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
                v_code  := sqlcode;
                v_errm  := sqlerrm; -- substr(sqlerrm, 1, 150);
                putlog(pi_control_id, cn_proc || '|end_error|' || v_id || '|' || sqlerrm);
--                insis_cust_lpv.sys_schema_utils.update_poller_process_status(pi_control_id, 'ERROR');

                ins_error_stg(pi_control_id, v_id, 'ERR', 0, v_errm,
                              l_errmsg);
                ROLLBACK;
            END;
    END upload_row_pol;


    --------------------------------------------------------------------------------
    -- Name: upload_row_cov
    --------------------------------------------------------------------------------
    -- Purpose: Load info from file to detail table  
    --------------------------------------------------------------------------------

    PROCEDURE upload_row_cov (
        pi_control_id     fa_migr_spf_mp_cov.control_id%TYPE,
        pi_stag_id        fa_migr_spf_mp_cov.stag_id%TYPE,
        pi_policy_no      fa_migr_spf_mp_cov.policy_no%TYPE,
        pi_plan           fa_migr_spf_mp_cov.plan%TYPE,
        pi_cover_type     fa_migr_spf_mp_cov.cover_type%TYPE,
        pi_cover_name     fa_migr_spf_mp_cov.cover_name%TYPE,
        pi_insured_value  fa_migr_spf_mp_cov.insured_value%TYPE
    ) IS

        cn_proc  VARCHAR2(100) := 'upload_row_cov:' || pi_control_id;
        v_code   VARCHAR(4000);
        v_errm   VARCHAR(4000);
        v_id     NUMBER;
    BEGIN
--        putlog(pi_control_id, cn_proc || '|start|params: ' || pi_policy_no||','||pi_plan||','||pi_cover_type);
        BEGIN
            SELECT /*+ INDEX_DESC(stg fa_migr_spf_mp_cov_pk) */
                stag_id + 1
            INTO v_id
            FROM
                cust_migration.fa_migr_spf_mp_cov stg
            WHERE
                    control_id = pi_control_id
                AND ROWNUM = 1;

        EXCEPTION
            WHEN no_data_found THEN
                v_id := 1;
        END;

        INSERT INTO cust_migration.fa_migr_spf_mp_cov (
            control_id,
            stag_id,
            policy_no,
            plan,
            cover_type,
            cover_name,
            insured_value
        ) VALUES (
            pi_control_id,
            v_id,
            pi_policy_no,
            pi_plan,
            pi_cover_type,
            pi_cover_name,
            pi_insured_value
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
                v_code  := sqlcode;
                v_errm  := sqlerrm;
                putlog(pi_control_id, cn_proc || '|end_error|' || v_id || '|' || sqlerrm);
                --insis_cust_lpv.sys_schema_utils.update_poller_process_status(pi_control_id, 'ERROR');

                ins_error_stg(pi_control_id, v_id, 'ERR', 0, v_errm,
                              l_errmsg);
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
                INNER JOIN insis_gen_cfg_v10.cpr_params               p ON pv.param_id = p.param_cpr_id
            WHERE
                    pr.product_code = pi_product_code
                AND pr.status <> 'C'
                AND p.param_name = pi_param_name;

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
        v_fa_migr_spf_mp_pol    cust_migration.fa_migr_spf_mp_pol%rowtype;
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
        v_ret                   BOOLEAN := false;
        v_file_workers_count    NUMBER;
        v_file_avg_age_insured  NUMBER;
        v_policy_no_renov       insis_gen_v10.policy.policy_no%TYPE;
        l_agent_id              insis_people_v10.p_agents.agent_id%TYPE;
    BEGIN
        l_log_proc    := pi_control_id;
        putlog(pi_control_id, cn_proc || '|start|params: ' || pi_control_id || ',' || pi_file_id || ',' || pi_file_name);

        v_flag_error  := false;
        v_file_id     := pi_file_id;

        --SALES
--        FOR r_agent IN (
--                --SALES
--                SELECT UNIQUE agent_inx_id 
--                FROM cust_migration.fa_migr_spf_mp_pol 
--                WHERE control_id = complete_data.pi_control_id
--                AND att_master_policy_id IS NOT NULL
--                and agent_inx_id is not null

--                UNION

--                --INTAGCOLL
--                SELECT UNIQUE stg.collector_inx_id 
--                FROM cust_migration.fa_migr_spf_mp_pol stg
--                WHERE control_id = complete_data.pi_control_id
--                AND stg.att_master_policy_id IS NOT NULL
--                AND stg.collector_inx_id is not null
--
--        )
--        LOOP
--            BEGIN
--                SELECT
--                    agent_id
--                INTO l_agent_id
--                FROM
--                    insis_people_v10.p_agents
--                WHERE
--                    man_id = (
--                        SELECT
--                            man_id
--                        FROM
--                            insis_cust.intrf_lpv_people_ids
--                        WHERE
--                            insunix_code = r_agent.agent_inx_id
--                    );
--    
--            EXCEPTION
--                WHEN NO_DATA_FOUND THEN
--                    l_agent_id := null;
--                WHEN OTHERS THEN
--                    putlog('select agent_id.err:'||r_agent.agent_inx_id || ':' ||sqlerrm); 
----                    srv_error_set('select agent_id', 'InsrDurValidate_Agent', sqlerrm, pio_errmsg);
--            END;
--            
--            if l_agent_id is not null then
--                UPDATE cust_migration.fa_migr_spf_mp_pol d
--                SET
--                    d.ATT_AGENT_ID = l_agent_id
--                WHERE
--                        d.control_id = complete_data.pi_control_id
--                    AND d.agent_inx_id = r_agent.agent_inx_id;
--            end if;        
----            putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'agent_id, type:' || l_agent_id || ',' || l_agent_type); 
--        
--        END LOOP;
--
--        commit;


--        FOR r_provider IN (
--                SELECT UNIQUE stg.control_id, stg.provider_inx_id, 
--                              stg.att_provider_id --this field is null at this point
--                FROM cust_migration.fa_migr_spf_mp_pol stg
--                WHERE stg.control_id = complete_data.pi_control_id
--                AND stg.att_master_policy_id IS NOT NULL
--                AND stg.provider_inx_id is not null
--        )
--        LOOP
--            BEGIN
--                SELECT
--                    man_id
--                INTO 
--                    r_provider.att_provider_id
--                FROM
--                    insis_cust.intrf_lpv_people_ids
--                WHERE
--                    insunix_code = r_provider.provider_inx_id;
--    
--            EXCEPTION
--                WHEN NO_DATA_FOUND THEN
--                    r_provider.att_provider_id := null;
--                WHEN OTHERS THEN
--                    putlog('select provider_id.err:'||r_provider.provider_inx_id || ':' ||sqlerrm); 
----                    srv_error_set('select agent_id', 'InsrDurValidate_Agent', sqlerrm, pio_errmsg);
--            END;
--            
--            if r_provider.att_provider_id is not null then
--                UPDATE cust_migration.fa_migr_spf_mp_pol d
--                SET
--                    d.att_provider_id = r_provider.att_provider_id
--                WHERE
--                        d.control_id = r_provider.control_id
--                    AND d.provider_inx_id = r_provider.provider_inx_id;
--            end if;        
----            putlog(pi_fa_vley_row.control_id,pi_fa_vley_row.stag_id,'agent_id, type:' || l_agent_id || ',' || l_agent_type); 
--        
--        END LOOP;

--    --
--    --dependent_policy_info
--    --
--        v_step        := '[dependent_policy_info]';
--        FOR r_dependent_pol IN (
--            SELECT
--                *
--            FROM
--                cust_migration.fa_migr_spf_mp_pol
--            WHERE
--                    control_id = complete_data.pi_control_id
--                AND att_master_policy_id IS NOT NULL
--        ) LOOP
--            putlog(cn_proc || '|dependent_policy_info|policy_name: ' || r_dependent_pol.policy_name);
--            
--            l_err                                   := NULL;
--            v_fa_migr_spf_mp_pol                      := NULL;
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
----                                       v_fa_migr_spf_mp_pol.att_mpi_groupi_id, 
----                                       v_fa_migr_spf_mp_pol.att_mpi_subgroupi_id);
----
----        --if not (r_dependent_pol.att_operation_code = CN_OPER_EMI or r_dependent_pol.att_operation_code = CN_OPER_INC) then
----         putlog (cn_proc||' get_actual_data_policy_by_man_id' );
----         get_actual_data_policy_by_man_id(  r_dependent_pol.att_mpi_policy_id, 
----                                                              r_dependent_pol.policy_no,
----                                                              v_fa_migr_spf_mp_pol.att_man_id,
----                                                              v_fa_migr_spf_mp_pol.att_mdpi_actual_policy_id, 
----                                                              v_fa_migr_spf_mp_pol.att_mdpi_actual_annex_id,
----                                                              v_fa_migr_spf_mp_pol.att_mdpi_actual_worker_cat,
----                                                              v_fa_migr_spf_mp_pol.att_mdpi_actual_salary, 
----                                                              v_fa_migr_spf_mp_pol.att_mdpi_actual_adm_office );
--
--        --complete dependent related data
----            UPDATE cust_migration.fa_migr_spf_mp_pol d
----            SET policy_name = replace(policy_name, ' ', ''),
----                att_holder_man_id = r_dependent_pol.att_holder_man_id,
----              ...
----            att_mdpi_actual_policy_id = v_fa_migr_spf_mp_pol.att_mdpi_actual_policy_id,
----            att_mdpi_actual_annex_id = v_fa_migr_spf_mp_pol.att_mdpi_actual_annex_id,
----            att_mpi_groupi_id =  v_fa_migr_spf_mp_pol.att_mpi_groupi_id,
----            att_mpi_subgroupi_id =  v_fa_migr_spf_mp_pol.att_mpi_subgroupi_id,
----            WHERE
----                    control_id = r_dependent_pol.control_id
----                AND stag_id = r_dependent_pol.stag_id;
--
--        END LOOP;
--
--        COMMIT;

        putlog(pi_control_id, cn_proc || '|end');
        v_ret         := true;
        RETURN v_ret;
    EXCEPTION
        WHEN OTHERS THEN
            srv_error.setsyserrormsg(l_srverrmsg, 'fa_cust_migr_spf_mp.complete', sqlerrm, sqlcode);
            srv_error.seterrormsg(l_srverrmsg, v_pio_err);
            putlog(pi_control_id, cn_proc || '|end_error|' || sqlerrm);
            v_ret := false;
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
--        v_fields_val    cust_migration.fa_migr_spf_err.err_desc%TYPE;
        v_ret           BOOLEAN := false;
        v_object_type   insis_gen_v10.insured_object.object_type%TYPE;

    --
    --updates all detail records with error 
    -- to be used when one error block all dataset

        PROCEDURE update_stg_err_all (
            pi_control_id IN NUMBER
        ) AS
        BEGIN
            UPDATE cust_migration.fa_migr_spf_mp_pol
            SET
                att_status_rec = cn_stat_rec_error
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
                         cust_migration.fa_migr_spf_err e
                    INNER JOIN cust_migration.fa_migr_spf_mp_pol d ON ( e.control_id = d.control_id
                                                                        AND e.stag_id = d.stag_id )
                WHERE
                        e.control_id = update_stg_err_mp.pi_control_id
                    AND e.err_code = update_stg_err_mp.pi_err_code
            ) LOOP
            --updates with error all records related to a master policy  
                UPDATE cust_migration.fa_migr_spf_mp_pol
                SET
                    att_status_rec = cn_stat_rec_error
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
            UPDATE cust_migration.fa_migr_spf_mp_pol
            SET
                att_status_rec = cn_stat_rec_error
            WHERE
                    control_id = update_stg_err.pi_control_id
--     and (att_status_rec <> CN_STAT_REC_ERROR or 
                AND stag_id IN (
                    SELECT
                        stag_id
                    FROM
                        cust_migration.fa_migr_spf_err e
                    WHERE
                            e.control_id = update_stg_err.pi_control_id
                        AND e.err_code = update_stg_err.pi_err_code
                );

            putlog(pi_control_id, 'update_stg_err|' || pi_err_code || ':' || SQL%rowcount);
            COMMIT;
        END update_stg_err;

    --validate_data()
    BEGIN
        l_log_proc    := pi_control_id;
        putlog(pi_control_id, cn_proc || '|start|params: ' || pi_control_id || ',' || pi_file_id || ',' || pi_file_name);

        insis_sys_v10.insis_context.prepare_session (pi_app         => 'GEN',
                                                       pi_action      => null,
                                                       pi_username    => CN_PROCESS_USER, 
                                                       pi_user_role   => 'InsisStaff',
                                                       pi_lang        => NULL,
                                                       pi_country     => NULL);


        v_flag_error  := false;
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

    insert into cust_migration.fa_migr_spf_err(control_id, stag_id, err_code, err_desc, err_type)
    select d.control_id, d.stag_id, v_validation err_code, v_fields_val err_desc, 'fatal' err_type
    from cust_migration.fa_migr_spf_mp_pol d
    where control_id = pi_control_id
      and att_status_rec <> CN_STAT_REC_ERROR  
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

    insert into cust_migration.fa_migr_spf_err(control_id, stag_id, err_code, err_desc, err_type)
    select d.control_id, d.stag_id, v_validation err_code, v_fields_val err_desc, 'fatal' err_type
    from cust_migration.fa_migr_spf_mp_pol d
    where control_id = pi_control_id
      and att_status_rec <> CN_STAT_REC_ERROR  
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
        from cust_migration.fa_migr_spf_mp_pol
        where control_id = pi_control_id;

        if v_cant > 1 then  
            v_isFatalError := true;
            l_SrvErrMsg := null;
            v_pio_Err := null;

            putlog (cn_proc || '|' || v_validation || '|insr_type: ' ||   v_cant);

            srv_error.SetErrorMsg (l_SrvErrMsg, 'fa_cust_migr_spf_mpvalidate_data', 'lpv_policy_issuing_bo.init_val_master_policy_exist', 'Cant:'||v_cant);
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
                                        from cust_migration.fa_migr_spf_mp_pol 
                                      where control_id = pi_control_id
                                         and att_status_rec <> CN_STAT_REC_ERROR
                                         and ( att_mpi_insr_type is null  or  
                                                  (att_mpi_insr_type || '-' || att_mpi_as_is) not in 
                                                  ('2009-01', '2010-01', '2010-02', '2011-07') ) )
        loop

            l_SrvErrMsg := null;
            v_pio_Err := null;

            putlog ('validate_data|allowed_insr_type: ' ||  r_ins_obj_dtl.stag_id );

            v_isFatalError := true;

            update cust_migration.fa_migr_spf_mp_pol 
            set att_status_rec = CN_STAT_REC_ERROR
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

    insert into cust_migration.fa_migr_spf_err(control_id, stag_id, err_code, err_desc, err_type)
    select d.control_id, d.stag_id, v_validation err_code, '' err_desc, 'fatal' err_type
    from cust_migration.fa_migr_spf_mp_pol d
    where control_id = pi_control_id
      and att_status_rec <> CN_STAT_REC_ERROR  
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
        insert into cust_migration.fa_migr_spf_err(control_id, stag_id, err_code, err_desc, err_type)
        select control_id, stag_id, v_validation err_code, 'People older than 100 years' err_desc, 'fatal' err_type
        from cust_migration.fa_migr_spf_mp_pol d 
        where control_id = pi_control_id
          and att_status_rec <> CN_STAT_REC_ERROR  
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

    insert into cust_migration.fa_migr_spf_err(control_id, stag_id, err_code, err_desc, err_type)
    select d.control_id, d.stag_id, v_validation err_code, '' err_desc, 'warning' err_type
    from cust_migration.fa_migr_spf_mp_pol d
    where d.control_id = pi_control_id
      and d.att_status_rec <> CN_STAT_REC_ERROR  
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
                            from cust_migration.fa_migr_spf_err e
                            left join cust_migration.fa_migr_spf_mp_pol d 
                                    on (d.control_id = e.ctrl_id and
                                        d.stag_id = e.stag_id)    
                            where e.ctrl_id = pi_control_id 
                         )
    loop
        l_SrvErrMsg := null;
        v_pio_Err := null;

        putlog ('validate_data|get_policy_by_policy_no: ' ||  r_ins_obj_err.stag_id );

        srv_error.SetErrorMsg (l_SrvErrMsg, 'fa_cust_migr_spf_mp.validate_data', 'fa_cust_migr_spf_mp'||r_ins_obj_err.ERR_CODE, r_ins_obj_err.ERR_DESC); 
        srv_error.SetErrorMsg (l_SrvErrMsg, v_pio_Err);       

        v_errm := '[' || r_ins_obj_err.err_code|| '] ' || srv_error.ErrCollection2String(v_pio_Err) || '] '|| 'Doc_number [' || r_ins_obj_err.doc_number ||']';

        sys_schema_utils.log_poller_error_process(v_file_id, pi_file_name, 'XLS_INS_OBJ', v_errm, v_gstage);

    end loop;

    commit;

    --
*/
        UPDATE cust_migration.fa_migr_spf_mp_pol stg
        SET
            stg.att_status_rec = cn_stat_rec_valid
        WHERE
                stg.control_id = pi_control_id
            AND stg.att_status_rec <> cn_stat_rec_error;

        COMMIT;
        
        putlog(pi_control_id, cn_proc || '|end|' || SQL%rowcount);
        v_ret         := true;
        
        RETURN v_ret;
    
    EXCEPTION
        WHEN OTHERS THEN
            srv_error.setsyserrormsg(l_srverrmsg, 'fa_cust_migr_spf_mp.validate_data', sqlerrm, sqlcode);
            srv_error.seterrormsg(l_srverrmsg, v_pio_err);
            putlog(pi_control_id, cn_proc || '|end_error|' || sqlerrm);
            v_ret := false;
            RETURN v_ret;
    END validate_data;

    --
    --updates policy_id created 
    --

    PROCEDURE upd_new_policy_id (
        pi_control_id  fa_migr_spf_mp_pol.control_id%TYPE,
        pi_dtl_stg_id  fa_migr_spf_mp_pol.stag_id%TYPE,
        pi_policy_id   fa_migr_spf_mp_pol.att_policy_id%TYPE
    ) IS
    BEGIN
        UPDATE fa_migr_spf_mp_pol
        SET
            att_policy_id = pi_policy_id
        WHERE
                control_id = pi_control_id
            AND stag_id = pi_dtl_stg_id;

    EXCEPTION
        WHEN OTHERS THEN
            putlog(pi_control_id, 'upd_new_policy_id|error|' || sqlerrm);
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
        pi_fa_migr_pol_row  IN   cust_migration.fa_migr_spf_mp_pol%rowtype,
        --po_errs             OUT  srverr
        pio_errmsg          IN OUT  srverr
    ) IS

        cn_proc                         VARCHAR2(100) := 'process_row_' || pi_control_id || '_' || pi_fa_migr_pol_row.stag_id;
        cn_intagcoll_generic_id         insis_gen_v10.policy_agents.agent_id%type := 6010001261;
        
        
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
        
        l_plan_desc                     VARCHAR2(100);
        l_plan_prev                     VARCHAR2(100);
            
            --TODO: pasar como campo atributo
        l_policy_name                   insis_gen_v10.policy.policy_name%TYPE;
        l_policy_id_dependent           insis_gen_v10.policy.policy_id%TYPE;
        l_payment_frecuency_code        NUMBER(2);
        l_user_name                     insis_people_v10.policy.username%TYPE;
        l_result                        BOOLEAN;
        l_master_policy_id              insis_gen_v10.policy.policy_id%TYPE;
        l_agent_id                      insis_people_v10.p_agents.agent_id%TYPE;
        l_agent_id_directos             insis_people_v10.p_agents.agent_id%TYPE;
        l_agent_type                    insis_people_v10.p_agents.agent_type%TYPE;
        l_internal_agent_id             insis_people_v10.p_agents.agent_id%TYPE;
        l_engagement_id                 insis_gen_v10.policy_engagement.engagement_id%TYPE;
        l_insr_begin                    insis_gen_v10.policy.insr_begin%TYPE;
        l_office_type                   insis_people_v10.pp_office_type;
        l_office_id                     insis_gen_v10.policy.office_id%TYPE;
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
        l_tech_branch                   insis_gen_v10.policy.attr1%TYPE;
        l_sbs_code                      insis_gen_v10.policy.attr2%TYPE;
        
        l_curr_plan_id                  pls_integer;
    
        CURSOR c_object_types(
            pi_insr_type    insis_cust.cfglpv_groups_allowance.insr_type%type, 
            pi_as_is        insis_cust.cfglpv_groups_allowance.as_is_product%type) is 
        SELECT
              gr.object_type, 
              --names are translated to spanish
                replace(
                replace(
                replace(
                replace(
                replace(
                replace(
                replace(
                replace(typ.name,
                    'Main',     'Principal'), 
                    'Spouse',   'Conyuge'),
                    'Children', 'Hijo'),
                    'Relatives', 'Familiar' ),
                    'Relative', 'Familiar' ),
                    'Optional Members','Adicional'),
                    'Opt Memb','Adicional'),
                    'Parents','Padres') name
          FROM
              insis_cust.cfglpv_groups_allowance gr
              INNER JOIN insis_gen_v10.hst_object_type typ ON ( gr.object_type = typ.id )
          WHERE
                  gr.insr_type = pi_insr_type
            AND gr.as_is_product = pi_as_is
            --exclude group record
            AND not (gr.mandatory = 'N' and gr.ref_group_object_type IS NULL)
            order by object_type;
            
        l_object_types_rec  c_object_types%rowtype;

        
        
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
                    insis_people_v10.p_people p
                    inner join insis_people_v10.p_clients c on (c.man_id = p.man_id)
                WHERE
                    p.egn = pi_egn;
    
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
                    putlog(pi_fa_migr_pol_row.control_id, '--get_sbs_techbr.err:' || sqlerrm);
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
                    LEFT JOIN insis_cust.cfg_nom_language_table      lan ON ( lan.id = rel.rel_id )
                WHERE
                        1 = 1
                    AND lan.table_name LIKE '%HT_PEOPLE_RELATION%'
                    AND name LIKE '%' || upper(pi_rel_desc) || '%';

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
--            putlog(pi_control_id, '--update_conditions:' || pi_cond_type);
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
                putlog(pi_control_id, '--update_conditions.err:' || sqlerrm);
                srv_error_set('update_conditions', 'SYSERROR', sqlerrm, pio_errmsg);
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
--            putlog(pi_control_id, '--GET_POL_QUEST|' || pi_policy_id || ':' || pi_quest_code || ':' || pi_quest_answer);

            IF pi_policy_id IS NULL THEN
                return;
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
                return;
            END IF;        
        
            --UPD_QUEST

--            putlog(pi_control_id, '--UPD_QUEST');
            insis_sys_v10.srv_context.setcontextattrnumber(pio_outcontext, 'ID', insis_sys_v10.srv_context.integers_format, insis_sys_v10.
            srv_question_data.gquestionrecord.id);

            insis_sys_v10.srv_context.setcontextattrchar(pio_outcontext, 'QUEST_ANSWER', pi_quest_answer);
            insis_sys_v10.srv_events.sysevent('UPD_QUEST', pio_outcontext, pio_outcontext, pio_errmsg);
            IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
                putlog(pi_control_id, '--UPD_QUEST.err:' || srv_error.errcollection2string(pio_errmsg));
                return;
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
--            putlog(pi_control_id, '--GET_INSURED_QUEST|' || pi_insured_id || ':' || pi_quest_code || ':' || pi_quest_answer);

            IF pi_insured_id IS NULL THEN
                return;
            END IF;
            
            --GET_INSURED_QUEST
            insis_sys_v10.srv_prm_quest.sinsuredobjid(pio_outcontext, pi_insured_id);
            insis_sys_v10.srv_prm_quest.squestcode(pio_outcontext, pi_quest_code);
            insis_sys_v10.srv_events.sysevent('GET_INSURED_QUEST', pio_outcontext, pio_outcontext, pio_errmsg);
            IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
                putlog(pi_control_id, '--GET_INSURED_QUEST.err:' || srv_error.errcollection2string(pio_errmsg));
                return;
            END IF;        
        
            --UPD_QUEST

--            putlog(pi_control_id, '--UPD_QUEST');
            insis_sys_v10.srv_context.setcontextattrnumber(pio_outcontext, 'ID', insis_sys_v10.srv_context.integers_format, insis_sys_v10.
            srv_question_data.gquestionrecord.id);

            insis_sys_v10.srv_context.setcontextattrchar(pio_outcontext, 'QUEST_ANSWER', pi_quest_answer);
            insis_sys_v10.srv_events.sysevent('UPD_QUEST', pio_outcontext, pio_outcontext, pio_errmsg);
            IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
                putlog(pi_control_id, '--UPD_QUEST.err:' || srv_error.errcollection2string(pio_errmsg));
                return;
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
                    putlog(pi_control_id, cn_proc || '|Err.Record_Participant|--INS_RELATION');
                    return;
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
                        return;
                    END IF;
                END IF;

            END IF;

        END upd_people_relation;

    BEGIN
        l_log_proc := pi_fa_migr_pol_row.control_id || '-' || pi_fa_migr_pol_row.stag_id;
        putlog(pi_fa_migr_pol_row.control_id, 'process_row|start|' || pi_fa_migr_pol_row.policy_no);

        EXECUTE IMMEDIATE 'alter session set NLS_NUMERIC_CHARACTERS = ''.,''';
        
        insis_sys_v10.insis_context.prepare_session (pi_app         => 'GEN',
                                                       pi_action      => null,
                                                       pi_username    => CN_PROCESS_USER,
                                                       pi_user_role   => 'InsisStaff',
                                                       pi_lang        => NULL,
                                                       pi_country     => NULL);



        l_outcontext  := srvcontext();
        
        --todo: cargar client_id en columna ATT
        l_client_id := get_client_id_by_egn(pi_fa_migr_pol_row.holder_egn);
        
        putlog(pi_fa_migr_pol_row.control_id, 'l_client_id:'||l_client_id); 

-- todo: pasar agent_type a campo att_internal_agent_type (nuevo)
        l_agent_id := pi_fa_migr_pol_row.internal_agent_id;
--        l_agent_type := pi_fa_migr_pol_row.att_internal_agent_type;
--
        putlog(pi_fa_migr_pol_row.control_id, 'select agent_id'); 
        BEGIN
            SELECT
                agent_id, agent_type
            INTO l_agent_id, l_agent_type
            FROM
                insis_people_v10.p_agents
            WHERE
                agent_id = pi_fa_migr_pol_row.internal_agent_id
            ;

        EXCEPTION
            WHEN OTHERS THEN
                putlog(pi_fa_migr_pol_row.control_id, 'select agent_id.err:'||sqlerrm); 
                srv_error_set('select agent_id', 'InsrDurValidate_Agent', sqlerrm, pio_errmsg);
                return;
        END;
--        putlog(pi_fa_migr_pol_row.control_id, 'agent_id, type:' || l_agent_id || ',' || l_agent_type); 

        --TO_CHAR() in else section is necessary, otherwise it raise error and lpad doesn't work
        l_office_type := insis_people_v10.pp_office_type(case when LENGTH(pi_fa_migr_pol_row.office_id) < 2 
                                                                    then LPAD(pi_fa_migr_pol_row.office_id, 2, '0') 
                                                                    else to_char(pi_fa_migr_pol_row.office_id) 
                                                             end); 
        if l_office_type is not null and 
           l_office_type.office_id is not null then

            l_office_id := l_office_type.office_id;
        else
            putlog(pi_fa_migr_pol_row.control_id, 'err.office_id not found:'||pi_fa_migr_pol_row.office_id); 
        end if;
        

        ----###
        putlog(pi_fa_migr_pol_row.control_id, '--CREATE_ENGAGEMENT');
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ENGAGEMENT_ID', insis_sys_v10.srv_context.integers_format, NULL);
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'CLIENT_ID', insis_sys_v10.srv_context.integers_format, l_client_id);
        insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'ENGAGEMENT_STAGE', insis_gen_v10.gvar_pas.at_appl);
        insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'ENGAGEMENT_TYPE', insis_gen_v10.gvar_pas.eng_type_engagement);
        insis_sys_v10.srv_events.sysevent('CREATE_ENGAGEMENT', l_outcontext, l_outcontext, pio_errmsg);
        IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
            putlog(pi_fa_migr_pol_row.control_id, '--CREATE_ENGAGEMENT.err:' || srv_error.errcollection2string(pio_errmsg));
            return;
        END IF;

        insis_sys_v10.srv_context.getcontextattrnumber(l_outcontext, 'ENGAGEMENT_ID', l_engagement_id);

        ----###
        putlog(pi_fa_migr_pol_row.control_id, '--CREATE_ENG_POLICY');
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ENGAGEMENT_ID', insis_sys_v10.srv_context.integers_format, l_engagement_id);
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'INSR_TYPE', insis_sys_v10.srv_context.integers_format, pi_fa_migr_pol_row.insr_type);
        insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'POLICY_TYPE', insis_gen_v10.gvar_pas.engpoltype_master);
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_ID_ORG', insis_sys_v10.srv_context.integers_format, NULL);
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_STAGE', insis_sys_v10.srv_context.integers_format, insis_gen_v10.gvar_pas.define_applprep_state);
        --insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'AGENT_ID', insis_sys_v10.srv_context.integers_format,l_internal_agent_id);

        --In case of "Asesor" agent, it is necesary to include "Directos" internal agent
        --todo:usar contantes

        IF l_agent_type = 5 THEN --broker
            putlog(pi_fa_migr_pol_row.control_id, 'select agent_id 1412');
            BEGIN
                SELECT
                    agent_id
                INTO l_agent_id_directos
                FROM
                    insis_people_v10.p_agents
                WHERE
                    agent_no = '1412';

            EXCEPTION
                WHEN OTHERS THEN
                    putlog(pi_fa_migr_pol_row.control_id, 'agent_id1412.err:' || sqlerrm);
                    srv_error_set('select agent_id_1412', 'InsrDurValidate_Agent', sqlerrm, pio_errmsg);
                    return;
            END;

            insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'AGENT_ID', insis_sys_v10.srv_context.integers_format, l_agent_id_directos);--DIRECTOS            

        ELSE
            insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'AGENT_ID', insis_sys_v10.srv_context.integers_format, l_agent_id);
        END IF;
        insis_sys_v10.srv_events.sysevent('CREATE_ENG_POLICY', l_outcontext, l_outcontext, pio_errmsg);

        IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
            putlog(pi_fa_migr_pol_row.control_id, 'CREATE_ENG_POLICY.err:' || srv_error.errcollection2string(pio_errmsg));
            return;
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
        insis_gen_v10.pol_values.calcduration(l_begin_date, l_end_date, pi_fa_migr_pol_row.insr_type, l_calc_duration, l_calc_dimension);
--        end if;

        putlog(pi_fa_migr_pol_row.control_id, 'update policy:' || l_calc_duration || ',' || l_calc_dimension);
        get_sbs_techbr(pi_fa_migr_pol_row.insr_type, pi_fa_migr_pol_row.as_is, l_tech_branch, l_sbs_code);
        
        --todo: usa object type, o %rowtype
        BEGIN
            UPDATE insis_gen_v10.policy
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
    --            attr5 = pi_fa_migr_pol_row.frequency,  --todo
                payment_type = 'S', --single premium
                username = CN_POLICY_USER
            WHERE
                policy_id = l_master_policy_id;
        EXCEPTION
            WHEN OTHERS THEN
                putlog(pi_fa_migr_pol_row.control_id, 'Update_policy.err:'||sqlerrm); 
                srv_error_set('update_policy', null, sqlerrm, pio_errmsg);
                return;
        END;
    
        putlog(pi_fa_migr_pol_row.control_id, '--INS_POLICY_AGENTS.intagcoll');
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_AGENT_ID', insis_sys_v10.srv_context.integers_format, NULL);
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'AGENT_ID', insis_sys_v10.srv_context.integers_format, CN_INTAGCOLL_GENERIC_ID);
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ANNEX_ID', insis_sys_v10.srv_context.integers_format, insis_gen_v10.gvar_pas.def_annx_id);
        insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'AGENT_ROLE', 'INTAGCOLL');
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'COMM_SHARE', insis_sys_v10.srv_context.integers_format, 100);
        insis_sys_v10.srv_context.setcontextattrdate(l_outcontext, 'VALID_FROM', insis_sys_v10.srv_context.date_format, l_begin_date);
        insis_sys_v10.srv_events.sysevent('INS_POLICY_AGENTS', l_outcontext, l_outcontext, pio_errmsg);

        IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
            putlog(pi_fa_migr_pol_row.control_id, 'INS_POLICY_AGENTS.err:' || srv_error.errcollection2string(pio_errmsg));
            return;
        END IF;

    
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
        insis_gen_v10.srv_policy_data.gengagementbillingrecord.attr1 := case when lower(pi_fa_migr_pol_row.billing_type) like 'individual' then insis_cust.gvar_cust.BLC_BILL_TYPE_CL_IND  
                                                                        when lower(pi_fa_migr_pol_row.billing_type) like 'grupal' then insis_cust.gvar_cust.BLC_BILL_TYPE_CL_GROUP 
                                                                        else null end;
        insis_gen_v10.srv_policy_data.gengagementbillingrecord.payment_way := case when lower(pi_fa_migr_pol_row.payment_way_name) like 'cash' then 1  
                                                                                   when lower(pi_fa_migr_pol_row.payment_way_name) like 'direct debit' then 4
                                                                                   when lower(pi_fa_migr_pol_row.payment_way_name) like 'collection account' then 3 --bank transfer
                                                                              else insis_gen_v10.srv_policy_data.gengagementbillingrecord.payment_way end;
        --update 
        l_result    := insis_gen_v10.srv_policy_data.gengagementbillingrecord.updatepengagementbilling(pio_errmsg);
        IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
            putlog(pi_fa_migr_pol_row.control_id, 'updatepengagementbilling.err:' || srv_error.errcollection2string(pio_errmsg));
            return;
        END IF;
        
-- Plan    | Cover
-- Plan 1  | Cov1      
-- Plan 1  | Cov2  
-- Plan 2  | Cov1   
-- 
-- Hierachy: Plan -> Object -> Cover
-- Plan 1   
--    Main   
--        Cov1      
--        Cov2      
--    Add    
--        Cov1
--        Cov2
-- Plan 2   
--    Main   
--        Cov1      
-- ...
--  *Object: it's read from product configuration
--
        putlog(pi_fa_migr_pol_row.control_id, 'starting cover loop' );
        
        l_curr_plan_id := 0;

        for l_fa_migr_cov_rec in (select unique cv.control_id, cv.policy_no, cv.plan
                                from fa_migr_spf_mp_cov cv
                                where cv.control_id = pi_fa_migr_pol_row.control_id 
                                  and cv.policy_no = pi_fa_migr_pol_row.policy_no
                                order by cv.plan)
        loop
            --on every plan's change, a new one is creates
--            if l_fa_migr_cov_rec.plan <> nvl(l_plan_prev,'x') then
            
--                l_plan_prev         := l_fa_migr_cov_rec.plan;
                l_curr_plan_id      := l_curr_plan_id + 1;
                l_group_ins_obj_id  := null;

                BEGIN
                    OPEN c_object_types(pi_fa_migr_pol_row.insr_type, pi_fa_migr_pol_row.as_is);
                    
                    LOOP
                        FETCH c_object_types INTO l_object_types_rec;
                        EXIT WHEN c_object_types%notfound;
                
                        --Object name samples:
                        --"Prot Fam Vida Eterna -Main" -> "Plan 1-Main"
                        --"Sepelium - Optional Members" -> "Plan 3- Optional Members"
                        
                        --l_plan_desc := l_fa_migr_cov_rec.plan || l_object_types_rec.name; 
                        l_plan_desc := l_fa_migr_cov_rec.plan || ' ' || case when instr(l_object_types_rec.name, '-') = 0 
                                                                        then null 
                                                                        else substr(l_object_types_rec.name, instr(l_object_types_rec.name, '-'), 100) 
                                                                        end;
                        
--                        putlog(pi_fa_migr_pol_row.control_id, 'Object type:'|| l_object_types_rec.object_type);
        
                    
                        putlog(pi_fa_migr_pol_row.control_id,'--INS_GROUP_INS ['||l_curr_plan_id||','||l_plan_desc||']' );
            
                        --set to null to get a new value each time
                        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'OBJECT_ID', insis_sys_v10.srv_context.integers_format, null);
                        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'OBJECT_TYPE', insis_sys_v10.srv_context.integers_format, l_object_types_rec.object_type);
                        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'MAIN_OBJECT_ID', insis_sys_v10.srv_context.integers_format, NULL);
        --                insis_sys_v10.srv_context.setcontextattrchar(l_outcontext, 'OGPP1', l_curr_plan_id); --todo: confirmar si quitar
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
--                        putlog(pi_fa_migr_pol_row.control_id,'--INSERT_INSURED_OBJECT' );
                        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_ID', insis_sys_v10.srv_context.integers_format, l_master_policy_id);
                        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ANNEX_ID', insis_sys_v10.srv_context.integers_format, insis_gen_v10.gvar_pas.def_annx_id);
                        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'INSR_TYPE', insis_sys_v10.srv_context.integers_format, pi_fa_migr_pol_row.insr_type);
                        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'OBJECT_ID', insis_sys_v10.srv_context.integers_format, l_object_id);
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
                                putlog(pi_fa_migr_pol_row.control_id, 'INSERT_INSURED_OBJECT.err:'||sqlerrm); 
                                srv_error_set('update_insured_object', null, sqlerrm, pio_errmsg);
                                exit;
                        END;
                        putlog(pi_fa_migr_pol_row.control_id,'l_ins_obj_id: '||l_ins_obj_id);
                        
                        --the first insured object id is recorded to assign as group id to the next object
                        IF l_group_ins_obj_id IS NULL THEN
                            l_group_ins_obj_id := l_ins_obj_id;
                        END IF;
                    
                        ---
                        --- Selecting specific covers is disabled by now, to load all product's covers
                        ---
                        
--                        putlog(pi_fa_migr_pol_row.control_id,'--FILL_COVERS_FOR_SELECT' );
--                        
--                        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'INSURED_OBJ_ID', insis_sys_v10.srv_context.integers_format, l_ins_obj_id);
--                        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_ID', insis_sys_v10.srv_context.integers_format, l_master_policy_id);
--                        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ANNEX_ID', insis_sys_v10.srv_context.integers_format, insis_gen_v10.gvar_pas.def_annx_id);
--                        
--                        insis_sys_v10.srv_events.sysevent('FILL_COVERS_FOR_SELECT', l_outcontext, l_outcontext, pio_errmsg);
--                        insis_sys_v10.srv_context.getcontextattrchar(l_outcontext, 'PROCEDURE_RESULT', l_procedure_result);
--                        IF upper(l_procedure_result) = 'FALSE' THEN
--                            putlog(pi_fa_migr_pol_row.control_id, 'FILL_COVERS_FOR_SELECT.err:'||srv_error.ErrCollection2String(pio_errmsg)); 
--                            srv_error_set('fill_covers_for_select', null, 'event_return_false', pio_errmsg);
--                            return;
--                        END IF;
--        
--                        ---
--                        putlog(pi_fa_migr_pol_row.control_id,'--SELECTING_ALL_COVERS' );
--                        
--                        --todo : usar rutina generica. recbir valores, y nombre cobertura: if valores no nulos update ... where cover_type = ...
--                        --todo:usar bulk. cargar coberturas al obtener plan
--                        UPDATE insis_gen_v10.gen_covers_select gcs
--                        SET
--                            apply_cover = 1
--                        WHERE
--                            gcs.policy_id = l_master_policy_id
--                        and gcs.cover_type in (select cover_type from fa_migr_spf_mp_cov fa_cov 
--                                               where fa_cov.control_id = pi_fa_migr_pol_row.control_id
--                                                 and fa_cov.plan       = l_fa_migr_cov_rec.plan
--                                                 and fa_cov.policy_no  = l_fa_migr_cov_rec.policy_no
--                                                )
--                        ;
--        
--                        ---
--                        putlog(pi_fa_migr_pol_row.control_id,'--ATTACH_SELECTED_COVERS' );
--                        
--                        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'INSURED_OBJ_ID', insis_sys_v10.srv_context.integers_format, l_ins_obj_id);
--                        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_ID', insis_sys_v10.srv_context.integers_format, l_master_policy_id);
--                        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ANNEX_ID', insis_sys_v10.srv_context.integers_format, insis_gen_v10.gvar_pas.def_annx_id);
--                            
--                        insis_sys_v10.srv_events.sysevent('ATTACH_SELECTED_COVERS', l_outcontext, l_outcontext, pio_errmsg);
--                        insis_sys_v10.srv_context.getcontextattrchar(l_outcontext, 'PROCEDURE_RESULT', l_procedure_result);
--        
--                        IF upper(l_procedure_result) = 'FALSE' THEN
--                            putlog(pi_fa_migr_pol_row.control_id, 'ATTACH_SELECTED_COVERS.err:'||srv_error.ErrCollection2String(pio_errmsg)); 
--                            srv_error_set('attach_selected_covers', null, 'event_return_false', pio_errmsg);
--                            return;
--                        END IF;
        
--                        putlog(pi_fa_migr_pol_row.control_id, 'UPDATE tariff_percent' );
                        
                        UPDATE insis_gen_v10.gen_risk_covered grc
                        SET 
                            currency       = pi_fa_migr_pol_row.currency,
                            insured_value  = nvl((select fa_cov.insured_value 
                                                    from fa_migr_spf_mp_cov fa_cov 
                                                   where fa_cov.control_id  = l_fa_migr_cov_rec.control_id
                                                     and fa_cov.plan        = l_fa_migr_cov_rec.plan
                                                     and fa_cov.policy_no   = l_fa_migr_cov_rec.policy_no
                                                     and fa_cov.cover_type  = grc.cover_type), 
                                                 grc.insured_value)
        --                    tariff_percent        = case when cover_type = 'NATDEATHSL' then l_tariff_percent else tariff_percent end,
                            --Percent (Annual)
--                            manual_prem_dimension = gvar_pas.PREM_DIM_P 
                        WHERE
                                insured_obj_id = l_ins_obj_id 
                            ;
                    
                    END LOOP; --c_object_types
                                
                EXCEPTION
                    WHEN OTHERS THEN
                        srv_error_set('c_object_types', null, sqlerrm, pio_errmsg);
                        putlog(pi_fa_migr_pol_row.control_id, 'c_object_types.err:'||sqlerrm); 
                END;
                
                CLOSE c_object_types;

                IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
                    EXIT;
                END IF;
                
--            END IF;        
            
            IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
                EXIT;
            END IF;
                
        
        END LOOP; --l_fa_migr_cov_rec

        --exit when error found in loops
        IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
            return;
        end if;
            
        
--        putlog(pi_fa_migr_pol_row.control_id, '--FILL_POLICY_CONDITIONS');
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ANNEX_ID', insis_sys_v10.srv_context.integers_format, insis_gen_v10.gvar_pas.def_annx_id);
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_ID', insis_sys_v10.srv_context.integers_format, l_master_policy_id);
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'INSR_TYPE', insis_sys_v10.srv_context.integers_format, pi_fa_migr_pol_row.insr_type);
        insis_sys_v10.srv_events.sysevent('FILL_POLICY_CONDITIONS', l_outcontext, l_outcontext, pio_errmsg);
        insis_sys_v10.srv_context.getcontextattrchar(l_outcontext, 'PROCEDURE_RESULT', l_procedure_result);
        IF upper(l_procedure_result) = 'FALSE' THEN
            putlog(pi_fa_migr_pol_row.control_id, 'FILL_POLICY_CONDITIONS.err:' || srv_error.errcollection2string(pio_errmsg));
            srv_error_set('fill_policy_conditions', null, 'event_return_false', pio_errmsg);
            return;
        END IF;

        --
        -- policy_condition updates
        --

        putlog(pi_fa_migr_pol_row.control_id, '--UPDATING POLICY_CONDITION');
        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, 'AS_IS_SPF', pi_fa_migr_pol_row.as_is, NULL, 
                          l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, 'EMPL_LPV', yn_to_num(pi_fa_migr_pol_row.lpv_employee_flag,2,1), null, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id,'GRUPO_ASEGURADO', pi_fa_migr_pol_row.insr_group, null, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, 'PROVEEDOR_ASISTENCIA', pi_fa_migr_pol_row.prov_value, null, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, 'PUNTO_VENTA', pi_fa_migr_pol_row.sales_module_id, null, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);
        update_conditions(pi_fa_migr_pol_row.control_id, pi_fa_migr_pol_row.stag_id, 'TIPO_VENTA', pi_fa_migr_pol_row.sales_channel_spf_id, null, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, pio_errmsg);

        --Updates insr_type description in policy_names
        --insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_ID', insis_sys_v10.srv_context.integers_format, l_master_policy_id);
        --insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ANNEX_ID', insis_sys_v10.srv_context.integers_format, insis_gen_v10.gvar_pas.def_annx_id);
        insis_sys_v10.srv_events.sysevent('CUST_COND_UPD', l_outcontext, l_outcontext, pio_errmsg );
        IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
            putlog(pi_fa_migr_pol_row.control_id, 'CUST_COND_UPD.err:' || srv_error.errcollection2string(pio_errmsg));
            return;
        END IF;
        ----

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
            return;
        END IF;

--        l_quest_answer := CASE WHEN pi_fa_migr_pol_row.epolicy_flag = 'Y' THEN
--                                    3
--                          ELSE 
--                                    4
--                          END;
--        update_quest('POL', 'EPOLR', l_quest_answer, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);

--        IF pi_fa_migr_pol_row.plan = 6 
--        THEN 
--            update_quest('POL', '2009.01', pi_fa_migr_pol_row.FE_NUM_SAL, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
--            update_quest('POL', '2009.02', pi_fa_migr_pol_row.FE_MAX_SI, l_master_policy_id, insis_gen_v10.gvar_pas.def_annx_id, l_outcontext, pio_errmsg);
--        END IF;

        ----

        putlog(pi_fa_migr_pol_row.control_id, '--INSERT_ENDORSEMENT');
        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'POLICY_ID', insis_sys_v10.srv_context.integers_format, l_master_policy_id);

        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext, 'ANNEX_ID', insis_sys_v10.srv_context.integers_format, insis_gen_v10.
        gvar_pas.def_annx_id);

        insis_sys_v10.srv_events.sysevent('INSERT_ENDORSEMENT', l_outcontext, l_outcontext, pio_errmsg);
        insis_sys_v10.srv_context.getcontextattrchar(l_outcontext, 'PROCEDURE_RESULT', l_procedure_result);
        IF upper(l_procedure_result) = 'FALSE' THEN
            putlog(pi_fa_migr_pol_row.control_id, 'INSERT_ENDORSEMENT.err');
            return;
        END IF;
        
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
            return;
        END IF;

        ----

        putlog(pi_fa_migr_pol_row.control_id, '--APPL_CONF');
        insis_sys_v10.srv_events.sysevent('APPL_CONF', l_outcontext, l_outcontext, pio_errmsg);
        IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
            putlog(pi_fa_migr_pol_row.control_id, 'APPL_CONF.err|' || srv_error.errcollection2string(pio_errmsg));
            return;
        END IF;                        

        ----

        IF pi_fa_migr_pol_row.policy_state_desc = CN_FINAL_STATUS_REGISTERED THEN
            putlog(pi_fa_migr_pol_row.control_id, '--APPL_CONV');
            insis_sys_v10.srv_events.sysevent('APPL_CONV', l_outcontext, l_outcontext, pio_errmsg);
            IF NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg) THEN
                putlog(pi_fa_migr_pol_row.control_id, 'APPL_CONV.err|' || srv_error.errcollection2string(pio_errmsg));
                return;
            END IF;

        END IF;
        
        putlog(pi_fa_migr_pol_row.control_id, 'Update policy final:'||l_master_policy_id);
        
        UPDATE insis_gen_v10.policy
        SET
            policy_name = pi_fa_migr_pol_row.policy_no--, --todo : validar si es necesario
--            date_covered = nvl(l_date_covered,date_covered),
        WHERE
            policy_id = l_master_policy_id;

        putlog(pi_fa_migr_pol_row.control_id, 'process_row|end');
    EXCEPTION
        WHEN OTHERS THEN
            srv_error.setsyserrormsg(l_srverrmsg, 'fa_cust_migr_spf_mp.process_row', sqlerrm, sqlcode);
            srv_error.seterrormsg(l_srverrmsg, pio_errmsg);
            putlog(pi_control_id, cn_proc || '|end_excep|' || sqlerrm);
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
                cust_migration.fa_migr_spf_mp_pol stg
            WHERE
                    stg.control_id = pi_control_id
                AND stg.stag_id BETWEEN pi_id_init AND pi_id_end
                AND stg.att_status_rec = CN_STAT_REC_VALID
            ORDER BY
                control_id,
                stag_id
        ) LOOP
            SAVEPOINT generate_job_sp;
            v_pio_err := NULL;
--            process_spf_data_record
            process_row(pi_control_id, r_ins_det, v_pio_err);
            IF srv_error.rqstatus(v_pio_err) THEN
                COMMIT;
            ELSE
--                ROLLBACK TO generate_job_sp;
                COMMIT; --todo: por ahora deja poliza para revisar error
                v_errm := srv_error.errcollection2string(v_pio_err);

                ins_error_stg(r_ins_det.control_id, r_ins_det.stag_id, 'ERR', 0, v_errm, v_pio_err);

                UPDATE cust_migration.fa_migr_spf_mp_pol stg
                SET
                    stg.att_status_rec = CN_STAT_REC_ERROR
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
        v_ret               BOOLEAN := false;
        CURSOR c_job_size (
            pi_num_jobs IN NUMBER
        ) IS
        SELECT
            round(COUNT(*) / pi_num_jobs) AS jobs_size
        FROM
            cust_migration.fa_migr_spf_mp_pol
        WHERE
            control_id = pi_control_id;

        --to-do: validate against cust_migration.fa_migr_spf_err where err_type = 'error'

        CURSOR c_errors_exist (
            pi_ctrol_id IN NUMBER
        ) IS
        SELECT
            COUNT(*) AS no_of_errors
        FROM
            cust_migration.fa_migr_spf_err
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
            WITH a AS (
                SELECT
                    stag_id,
                    ROW_NUMBER() OVER(PARTITION BY control_id
                        ORDER BY
                            stag_id
                    ) AS rownum_
                FROM
                    cust_migration.fa_migr_spf_mp_pol
                WHERE
                        control_id = pi_control_id
                    AND att_status_rec = cn_stat_rec_valid
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
                    a
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
            l_sql_stmt                := ' begin fa_cust_migr_spf_mp.process_job (' || pi_control_id || ', ' || v_file_id || ', ' || chr(39) || pi_file_name ||
            chr(39) || ', ' || c_st.fv || ', ' || c_st.lv || ', ' || c_st.page_num || '); end;';

            v_n_job                   := v_n_job + 1;
            l_job_name                := l_task_name || v_n_job;
            putlog(pi_control_id, 'generate_jobs|' || l_job_name || '|' || l_sql_stmt);
            
            v_jobdef_arr.extend;
            v_jobdef             := sys.job_definition(job_name     => '"' || l_job_name || '"', job_style => 'REGULAR', number_of_arguments => 0,
                                                       job_type     => 'PLSQL_BLOCK', job_action => l_sql_stmt,
                                                       start_date   => sysdate, 
                                                       enabled      => true, 
                                                       auto_drop    => true, 
                                                       comments     => 'fa_cust_migr_spf - SPF'--,
                                            --instance_id    => 0
                   );

            v_jobdef_arr(v_n_job)     := v_jobdef;
        END LOOP;

        putlog(pi_control_id, 'generate_jobs|waiting jobs...');
        dbms_scheduler.create_jobs(v_jobdef_arr, 'TRANSACTIONAL'); --TRANSACTIONAL STOpi_ON_FIRST_ERROR  ABSORB_ERRORS 
        dbms_lock.sleep(2);
        WHILE ( true ) LOOP
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
        v_ret         := true;
        RETURN v_ret;
    EXCEPTION
        WHEN OTHERS THEN
            putlog(pi_control_id, 'generate_jobs|end_error|' || sqlerrm);
            v_ret := false;
            RETURN v_ret;
    END generate_jobs;

    --
    -- set_report_status
    --

    PROCEDURE set_report_status (
        pi_control_id  cust_migration.fa_migr_spf_err.control_id%TYPE,
        pi_status      cust_migration.fa_migr_spf_err.err_type%TYPE
    ) AS
    BEGIN
        IF pi_status = cn_ready_for_rep THEN
            --creates record for report process
            INSERT INTO cust_migration.fa_migr_spf_err (
                control_id,
                stag_id,
                err_seq,
                err_type,
                err_code,
                err_mess
            ) VALUES (
                pi_control_id,
                0,
                0,
                pi_status,
                NULL,
                '--Batch ready for reporting--'
            );

        ELSE
            UPDATE cust_migration.fa_migr_spf_err
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

        v_fa_migr_spf_mp_pol  cust_migration.fa_migr_spf_mp_pol%rowtype;
        v_code                VARCHAR(4000);
        pio_err               srverr;
        v_errm                VARCHAR(4000);
        v_file_id             NUMBER;
        v_result              BOOLEAN := false;--
    BEGIN
    --starting point for log sequence
    --sample: 1200000000000 + 34223 => 1203422300000
        l_log_seq   := l_log_seq_ini + ( pi_control_id * 1000000 );
        l_log_proc  := pi_control_id;
        putlog(pi_control_id, 'process_main|start|params:' || pi_control_id || ',' || pi_file_id || ',' || pi_file_name);

        v_result    := true;

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
        v_result    := true;

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
            insis_cust_lpv.sys_schema_utils.log_poller_error_process(v_file_id, pi_file_name, cn_poller_code, sqlerrm, 'process_main');
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

        v_fa_migr_spf_mp_pol  cust_migration.fa_migr_spf_mp_pol%rowtype;
        v_code                VARCHAR(4000);
        pio_err               srverr;
        v_errm                VARCHAR(4000);
        v_file_id             NUMBER;
        v_result              BOOLEAN := false;--
    BEGIN
    --starting point for log sequence
    --sample: 1200000000000 + 34223 => 1203422300000
        l_log_seq   := l_log_seq_ini + ( pi_control_id * 1000000 );
        l_log_proc  := pi_control_id;
        DELETE sta_log
        WHERE
                table_name = CN_POLLER_OBJECT
            AND batch_id LIKE to_char(reverse_process.pi_control_id) || '%';

        DELETE cust_migration.fa_migr_spf_err
        WHERE
            control_id = reverse_process.pi_control_id; 

        --se actualizan policy_no generados para que no se dupliquen

        UPDATE insis_gen_v10.policy
        SET
            policy_no = substr(policy_id, 1, 4) || substr(policy_id, 7, 6)
--            policy_name = substr(policy_id, 1, 4) || substr(policy_id, 7, 6)
        WHERE
            policy_no IN (
                SELECT
                    fa.policy_no
                FROM
                    cust_migration.fa_migr_spf_mp_pol fa
                WHERE
                    fa.control_id = reverse_process.pi_control_id
            )
            AND policy_no <> substr(policy_id, 1, 4) || substr(policy_id, 7, 6);

        putlog(pi_control_id, 'reverse_process|start|params:' || pi_control_id || ',' || pi_file_id || ',' || pi_file_name);

        putlog(pi_control_id, 'reverse_process|updating att');
        UPDATE cust_migration.fa_migr_spf_mp_pol d
        SET
            att_status_rec = CN_STAT_REC_LOAD
--                ,
--                att_insr_type = null,
--                att_as_is = null,
--                ...
        WHERE
            control_id = reverse_process.pi_control_id;

        COMMIT;
        putlog(pi_control_id, 'reverse_process|end');
    EXCEPTION
        WHEN OTHERS THEN
            insis_cust_lpv.sys_schema_utils.log_poller_error_process(v_file_id, pi_file_name, cn_poller_code, sqlerrm, 'Process_SPF_Data');
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
                            cust_migration.fa_migr_spf_err ctrl
                        WHERE
                                stag_id = 0
                            AND err_seq = 0 --first record
                            AND err_type = cn_ready_for_rep --record ready for report
                            AND EXISTS (
                                SELECT
                                    1
                                FROM
                                    cust_migration.fa_migr_spf_mp_pol stg
                                WHERE
                                        stg.control_id = ctrl.control_id
                                    AND stg.att_status_rec IN (
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
            putlog(po_poller_id, 'get_last_report_proc|end_err| ' || sqlerrm);
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
            putlog(pi_control_id_rep, 'upd_last_report_proc|end_err|' || sqlerrm);
    END upd_last_report_proc;

END fa_cust_migr_spf_mp;