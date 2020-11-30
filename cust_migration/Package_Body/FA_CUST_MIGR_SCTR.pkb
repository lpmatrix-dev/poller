CREATE OR REPLACE PACKAGE BODY fa_cust_migr_sctr AS
    --------------------------------------------------------------------------------
    -- Name: FA_CUST_MIGR_SCTR
    -------------------------------------
    -- Purpose: Poller for SCTR (2010) product migration 
    -- Type: PACKAGE
    -- Versioning: 
    --     LPV-FRAMEND0     2020-03-09      creation
    --     LPV-FRAMEND0     2020-03-30      ISS020-Fix comm_share for ASESVAT 
    --     LPV-FRAMEND0     2020-03-30      ISS019-New code for Special Prem Premium
    --     LPV-FRAMEND0     2020-04-01      ISS024-Date datatype changed to varchar (upload_FA_CUST_MIGR_SCTR)
    --     LPV-FRAMEND0     2020-04-02      ISS028-Activity detail could be null
    --     LPV-FRAMEND0     2020-04-02      ISS029-Added Final policy status   
    --     LPV-FRAMEND0     2020-04-02      ISS030-Job to process only valid records
    --     LPV-FRAMEND0     2020-04-02      ISS025-Uses INSUNIX codes for client and broker
    --     LPV-FRAMEND0     2020-04-14      ISS040-Objects standarization 
    --     LPV-FRAMEND0     2020-04-14      ISS058-Fix value for manual Premium Dimension
    --     LPV-FRAMEND0     2020-04-14      ISS062-Fix electronic policy flag
    --     LPV-FRAMEND0     2020-04-14      ISS066-Fix tender policy flag
    --     LPV-FRAMEND0     2020-04-18      ISS069-Set Default Value For Tariff when no value in poller
    --     LPV-FRAMEND0     2020-09-28      ISS096-Remove mining flag 
    --     LPV-FRAMEND0     2020-09-28      ISS104-Added internal agent & economic group
    ---------------------------------------------------------------------------------

    l_log_seq_ini cust_migration.sta_log.rec_count%TYPE := 1300000000000;
    l_log_seq cust_migration.sta_log.rec_count%TYPE := l_log_seq_ini;
    l_log_proc cust_migration.sta_log.batch_id%TYPE;
    l_errseq cust_migration.fa_migr_sctr_err.errseq%TYPE := 0;
    lc_stat_rec_load fa_migr_sctr_stg.att_status%TYPE := '1'; --Loaded in staging table
    lc_stat_rec_valid fa_migr_sctr_stg.att_status%TYPE := '2'; --Valid for process / sucessful
    lc_stat_rec_error fa_migr_sctr_stg.att_status%TYPE := '3'; --invalid for process/errors during process

    --todo:evaluar si agregar resto de mensajes,o manejar como constantes
    --seria lo mejor por aplicar solo a migracion
    --messages id for validations
    lc_val_invalid_broker_id CONSTANT insis_gen_cfg_v10.srv_messages.msg_id%TYPE := 'WS.6002'; --'Upload_SctrMaster_Validation_Invalid_Broker';
    lc_group_type_def CONSTANT insis_gen_v10.policy_benefit_groups.group_type%TYPE := 'SEGMENT'; --Group type 'Customer segmentation'     
    lc_agent_type_broker CONSTANT PLS_INTEGER := 5;
    lc_policy_user CONSTANT VARCHAR2(20) := 'CUST_MIGRATION'; 
    
    --------------------------------------------------------------------------------
    -- Name: FA_CUST_MIGR_SCTR.putlog
    -------------------------------------
    -- Purpose: record information in log
    -- Type: PROCEDURE
    -- Versioning:
    --     LPV-FRAMEND0     2020-03-09      creation
    ---------------------------------------------------------------------------------

    PROCEDURE putlog (
        pi_sys_ctrl_id IN NUMBER,
        pi_stg_id IN NUMBER,
        pi_msg VARCHAR
    )
        IS
    BEGIN
        sta_utils.log_message(
            'FA_CUST_MIGR_SCTR',
            l_log_proc,
            l_log_seq,
            '[' ||pi_stg_id ||']' ||pi_msg
        );

        dbms_output.put_line('[' ||systimestamp ||']; FA_CUST_MIGR_SCTR[' ||l_log_seq ||'] ' ||pi_msg);

        l_log_seq   := l_log_seq + 1;
    END putlog;

    --------------------------------------------------------------------------------
    -- Name: FA_CUST_MIGR_SCTR.putlogcontext
    -------------------------------------
    -- Purpose: record information from context in log
    -- Type: PROCEDURE
    -- Status: ACTIVE
    -- Versioning:
    --     LPV-FRAMEND0     2020-03-09      creation
    ---------------------------------------------------------------------------------

    PROCEDURE putlogcontext ( p_sys_ctrl_id IN NUMBER,p_context srvcontext ) AS
        v_text VARCHAR2(4000);
    BEGIN
        FOR r IN p_context.first..p_context.last LOOP
            v_text   := v_text ||r ||']|' ||p_context(r).attrcode ||'|' ||p_context(r).attrtype ||'|' ||p_context(r).attrformat ||'|' ||p_context(r).attrvalue;
        END LOOP;

        putlog(p_sys_ctrl_id,0,v_text);
    END putlogcontext;

    --------------------------------------------------------------------------------
    -- Name: fa_cust_migr_sctr.srv_error_set
    -------------------------------------
    -- Purpose: create error in srverr object
    -- Type: PROCEDURE
    -- Status: ACTIVE
    -- Versioning:
    --     LPV-FRAMEND0     2020-04-14      creation
    ---------------------------------------------------------------------------------

    PROCEDURE srv_error_set (
        pi_fn_name IN VARCHAR2,
        pi_error_code IN VARCHAR2,
        pi_error_msg IN VARCHAR2,
        pio_errmsg IN OUT srverr
    ) AS
        l_errmsg srverrmsg;
    BEGIN
        insis_sys_v10.srv_error.seterrormsg(
            l_errmsg,
            pi_fn_name,
            nvl(pi_error_code,'SYSERROR'),
            pi_error_msg
        );

        insis_sys_v10.srv_error.seterrormsg(l_errmsg,pio_errmsg);
    EXCEPTION
        WHEN OTHERS THEN
            putlog(
                0,
                0,
                'srv_error_set.err|' ||pi_error_code ||'|' ||sqlerrm
            );
            srv_error.setsyserrormsg(l_errmsg,'srv_error_set',sqlerrm);
            srv_error.seterrormsg(l_errmsg,pio_errmsg);
    END srv_error_set;

    
    --------------------------------------------------------------------------------
    -- Name: FA_CUST_MIGR_SCTR.upload_FA_CUST_MIGR_SCTR
    -------------------------------------
    -- Purpose: record information from context in log
    -- Type: PROCEDURE
    -- Status: ACTIVE
    -- Versioning:
    --     LPV-FRAMEND0     2020-03-09      creation
    ---------------------------------------------------------------------------------

    PROCEDURE upload_file_data (
        pi_control_id IN fa_migr_sctr_stg.control_id%TYPE,
        pi_stag_id IN fa_migr_sctr_stg.stag_id%TYPE,
        pi_rowseq IN fa_migr_sctr_stg.rowseq%TYPE,
        pi_insis_product_code IN fa_migr_sctr_stg.insis_product_code%TYPE,
        pi_as_is_product_code IN fa_migr_sctr_stg.as_is_product_code%TYPE,
        pi_policy_state IN fa_migr_sctr_stg.policy_state%TYPE,
        pi_internal_agent_no IN fa_migr_sctr_stg.internal_agent_no%TYPE,
        pi_internal_agent_name IN fa_migr_sctr_stg.internal_agent_name%TYPE,
        pi_econo_group_code IN fa_migr_sctr_stg.econo_group_code%TYPE,
        pi_econo_group_name IN fa_migr_sctr_stg.econo_group_name%TYPE,
        pi_policy_name IN fa_migr_sctr_stg.policy_name%TYPE,
        pi_policy_holder_code IN fa_migr_sctr_stg.policy_holder_code%TYPE,
        pi_broker_code IN fa_migr_sctr_stg.broker_code%TYPE,
        pi_sales_channel_code IN fa_migr_sctr_stg.sales_channel_code%TYPE,
        pi_commiss_perc IN fa_migr_sctr_stg.commiss_perc%TYPE,
        pi_office_number IN fa_migr_sctr_stg.office_number%TYPE,
        pi_activity_code IN fa_migr_sctr_stg.activity_code%TYPE,
        pi_activity_detail IN fa_migr_sctr_stg.activity_detail%TYPE,
        pi_section_code IN fa_migr_sctr_stg.section_code%TYPE,
        pi_currency_code IN fa_migr_sctr_stg.currency_code%TYPE,
        pi_begin_date IN fa_migr_sctr_stg.begin_date%TYPE,
        pi_end_date IN fa_migr_sctr_stg.end_date%TYPE,
        pi_date_covered IN fa_migr_sctr_stg.date_covered%TYPE,
        pi_prem_period_code IN fa_migr_sctr_stg.prem_period_code%TYPE,
        pi_policy_salud IN fa_migr_sctr_stg.policy_salud%TYPE,
        pi_min_prem_issue IN fa_migr_sctr_stg.min_prem_issue%TYPE,
        pi_min_prem_attach IN fa_migr_sctr_stg.min_prem_attach%TYPE,
        pi_iss_exp_percentage IN fa_migr_sctr_stg.iss_exp_percentage%TYPE,
        pi_min_iss_expenses IN fa_migr_sctr_stg.min_iss_expenses%TYPE,
        pi_calculation_type IN fa_migr_sctr_stg.calculation_type%TYPE,
        pi_billing_type IN fa_migr_sctr_stg.billing_type%TYPE,
        pi_billing_way IN fa_migr_sctr_stg.billing_way%TYPE,
        pi_warranty_clause_flag IN fa_migr_sctr_stg.warranty_clause_flag%TYPE,
        pi_spec_pen_clause_flag IN fa_migr_sctr_stg.spec_pen_clause_flag%TYPE,
        pi_spec_pen_clause_detail IN fa_migr_sctr_stg.spec_pen_clause_detail%TYPE,
        pi_gratuity_flag IN fa_migr_sctr_stg.gratuity_flag%TYPE,
        pi_consortium_flag IN fa_migr_sctr_stg.consortium_flag%TYPE,
        pi_elec_pol_flag IN fa_migr_sctr_stg.elec_pol_flag%TYPE,
        pi_tender_flag IN fa_migr_sctr_stg.tender_flag%TYPE,
        pi_wc_rm1 IN fa_migr_sctr_stg.wc_rm1%TYPE,
        pi_rn_rm1 IN fa_migr_sctr_stg.rn_rm1%TYPE,
        pi_wc_rm2 IN fa_migr_sctr_stg.wc_rm2%TYPE,
        pi_wd_rm2 IN fa_migr_sctr_stg.wd_rm2%TYPE,
        pi_rn_rm2 IN fa_migr_sctr_stg.rn_rm2%TYPE,
        pi_wc_rm3 IN fa_migr_sctr_stg.wc_rm3%TYPE,
        pi_wd_rm3 IN fa_migr_sctr_stg.wd_rm3%TYPE,
        pi_rn_rm3 IN fa_migr_sctr_stg.rn_rm3%TYPE,
        pi_wc_rm4 IN fa_migr_sctr_stg.wc_rm4%TYPE,
        pi_wd_rm4 IN fa_migr_sctr_stg.wd_rm4%TYPE,
        pi_rn_rm4 IN fa_migr_sctr_stg.rn_rm4%TYPE,
        pi_wc_rm5 IN fa_migr_sctr_stg.wc_rm5%TYPE,
        pi_wd_rm5 IN fa_migr_sctr_stg.wd_rm5%TYPE,
        pi_rn_rm5 IN fa_migr_sctr_stg.rn_rm5%TYPE
    ) IS
        pio_errmsg srverr;
        v_id PLS_INTEGER;
    BEGIN
        l_log_proc   := pi_control_id;
        
        --todo: usar indice descendiente o enviar secuencia desde configuracion poller
        SELECT COUNT(0) + 1
        INTO
            v_id
        FROM cust_migration.fa_migr_sctr_stg
        WHERE control_id = pi_control_id;

        INSERT INTO fa_migr_sctr_stg (
            control_id,
            stag_id,
            rowseq,
            insis_product_code,
            as_is_product_code,
            policy_state,
            internal_agent_no,
            internal_agent_name,
            econo_group_code,
            econo_group_name,
            policy_name,
            policy_holder_code,
            broker_code,
            sales_channel_code,
            commiss_perc,
            office_number,
            activity_code,
            activity_detail,
            section_code,
            currency_code,
            begin_date,
            end_date,
            date_covered,
            prem_period_code,
            policy_salud,
            min_prem_issue,
            min_prem_attach,
            iss_exp_percentage,
            min_iss_expenses,
            calculation_type,
            billing_type,
            billing_way,
            warranty_clause_flag,
            spec_pen_clause_flag,
            spec_pen_clause_detail,
            gratuity_flag,
            consortium_flag,
            elec_pol_flag,
            tender_flag,
            wc_rm1,
            rn_rm1,
            wc_rm2,
            wd_rm2,
            rn_rm2,
            wc_rm3,
            wd_rm3,
            rn_rm3,
            wc_rm4,
            wd_rm4,
            rn_rm4,
            wc_rm5,
            wd_rm5,
            rn_rm5
        ) VALUES (
            pi_control_id,
            v_id,
            pi_rowseq,
            pi_insis_product_code,
            pi_as_is_product_code,
            pi_policy_state,
            pi_internal_agent_no,
            pi_internal_agent_name,
            pi_econo_group_code,
            pi_econo_group_name,
            pi_policy_name,
            pi_policy_holder_code,
            pi_broker_code,
            pi_sales_channel_code,
            pi_commiss_perc,
            pi_office_number,
            pi_activity_code,
            pi_activity_detail,
            pi_section_code,
            pi_currency_code,
            pi_begin_date,
            pi_end_date,
            pi_date_covered,
            pi_prem_period_code,
            pi_policy_salud,
            pi_min_prem_issue,
            pi_min_prem_attach,
            pi_iss_exp_percentage,
            pi_min_iss_expenses,
            pi_calculation_type,
            pi_billing_type,
            pi_billing_way,
            pi_warranty_clause_flag,
            pi_spec_pen_clause_flag,
            pi_spec_pen_clause_detail,
            pi_gratuity_flag,
            pi_consortium_flag,
            pi_elec_pol_flag,
            pi_tender_flag,
            pi_wc_rm1,
            pi_rn_rm1,
            pi_wc_rm2,
            pi_wd_rm2,
            pi_rn_rm2,
            pi_wc_rm3,
            pi_wd_rm3,
            pi_rn_rm3,
            pi_wc_rm4,
            pi_wd_rm4,
            pi_rn_rm4,
            pi_wc_rm5,
            pi_wd_rm5,
            pi_rn_rm5
        );

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            putlog(
                pi_stag_id,
                v_id,
                '--upload_FA_CUST_MIGR_SCTR' || sqlerrm
            );
    END upload_file_data;

    FUNCTION tdate ( pi_strdate VARCHAR2 ) RETURN DATE AS
        l_date DATE;
    BEGIN
        BEGIN
            l_date   := TO_DATE(pi_strdate,'dd/mm/yyyy');
        EXCEPTION
            WHEN OTHERS THEN
                l_date   := NULL;
        END;

        RETURN l_date;
    END tdate;

    PROCEDURE sctr_wrapper (
        pi_control_id IN NUMBER,
        pi_file_id IN NUMBER,
        pi_file_name IN VARCHAR2,
        pi_poller_name IN VARCHAR2
    ) IS

        v_file_id NUMBER;
        v_all_succeded NUMBER;
        v_exito VARCHAR2(10);
        l_srverrmsg insis_sys_v10.srverrmsg;
        v_errm VARCHAR(4000);
        l_validation_id NUMBER;
        l_insis_product_code_rev VARCHAR2(1);
        l_sales_channel_code_rev insis_cust.hst_cust_sales_unit.id%TYPE;
        l_office_number_rev insis_people_v10.p_offices.office_no%TYPE;
        l_currency_code_rev VARCHAR2(3);
        l_calculation_type_rev insis_gen_v10.hs_cond_dimension.id%TYPE;
        l_billing_type_rev insis_gen_v10.hs_cond_dimension.id%TYPE;
        l_billing_way_rev insis_gen_v10.hs_cond_dimension.id%TYPE;
        l_policy_holder_code_rev insis_people_v10.p_clients.client_id%TYPE;
        l_broker_code_rev insis_people_v10.p_agents.agent_id%TYPE;
        l_as_is_product_code_rev insis_gen_v10.hs_cond_dimension.id%TYPE;
        l_agent_type insis_people_v10.p_agents.agent_type%TYPE;
        l_internal_agent_type insis_people_v10.pp_agent_type;
        pio_err srverr;
        v_count_errors NUMBER;
        --JOB
        l_number_of_jobs NUMBER := 25;
        l_job_size NUMBER;
        newjobarr sys.job_definition_array;
        l_task_name VARCHAR(1000);
        l_sql_stmt VARCHAR2(1000);
        l_n_job NUMBER := 0;
        newjob sys.job_definition;
        l_job_name VARCHAR(1000);
        l_jobs_running NUMBER;
        CURSOR c_job_size ( pi_num_jobs IN NUMBER ) IS
            SELECT round(COUNT(*) / pi_num_jobs) AS jobs_size
            FROM cust_migration.fa_migr_sctr_stg
            WHERE control_id = pi_control_id;

    BEGIN
        l_log_proc              := pi_control_id;
        putlog(
            pi_control_id,
            0,
            'sctr_wrapper|start|params: ' ||pi_control_id ||',' ||pi_file_id ||',' ||pi_file_name
        );

        IF pi_file_id IS NULL
        THEN
            insis_cust_lpv.sys_schema_utils.log_poller_process(
                pi_control_id,
                pi_file_name,
                pi_poller_name,
                'Poller with Process ID ' || pi_control_id,
                v_file_id
            );
        ELSE
            v_file_id   := pi_file_id;
        END IF;

        UPDATE insis_cust_lpv.sys_poller_process_ctrl
            SET
                file_id = v_file_id
        WHERE sys_poller_process_ctrl_id = pi_control_id;

        putlog(pi_control_id,0,'sctr_wrapper|completing values');
        
        --updating internal agent_id
        FOR r_age_row IN (
            SELECT UNIQUE
                   internal_agent_no
            FROM cust_migration.fa_migr_sctr_stg
            WHERE control_id = pi_control_id AND internal_agent_no IS NOT NULL
        ) LOOP
            l_internal_agent_type   := insis_people_v10.pp_agent_type(r_age_row.internal_agent_no);
            IF
                l_internal_agent_type IS NOT NULL AND l_internal_agent_type.agent_id IS NOT NULL
            THEN
                
--                putlog(pi_control_id,0,'agent_no,internal_agent_id:'|| l_internal_agent_type.agent_id); 
                UPDATE cust_migration.fa_migr_sctr_stg
                    SET
                        att_int_agent_id = l_internal_agent_type.agent_id
                WHERE control_id = pi_control_id AND internal_agent_no = r_age_row.internal_agent_no;

            END IF;

        END LOOP;

        COMMIT;
        v_all_succeded          := 1;
        putlog(pi_control_id,0,'sctr_wrapper|looping values');
        FOR rec_sctr_master IN (
            SELECT *
            FROM cust_migration.fa_migr_sctr_stg
            WHERE control_id = pi_control_id
        ) LOOP
            v_exito   := 'OK';
            
            --QRY VALIDACION
            BEGIN
                SELECT 1 validation_id,
                       (
                        CASE
                            WHEN rec_sctr_master.insis_product_code = '2010' THEN 'Y'
                            ELSE
                                'N'
                        END
                    ) insr_type,
                       (
                        SELECT sc.id
                        FROM insis_cust.hst_cust_sales_unit sc
                        WHERE sc.status = 'A' AND sc.id = rec_sctr_master.sales_channel_code
                    ) sales_channel,
                       (
                        SELECT office_no
                        FROM insis_people_v10.p_offices
                        WHERE office_no =
                                CASE
                                    WHEN length(rec_sctr_master.office_number) = 1 THEN lpad(
                                        rec_sctr_master.office_number,
                                        2,
                                        '0'
                                    )
                                    ELSE
                                        TO_CHAR(rec_sctr_master.office_number)
                                END
                    ) office_no,
                       (
                        SELECT id
                        FROM insis_gen_cfg_v10.cfg_nom_language_table
                        WHERE table_name = 'HT_CURRENCY_TYPE' AND language = 'SPANISH' AND id = rec_sctr_master.currency_code
                    ) currency,
                       (
                        SELECT cd.id
                        FROM insis_gen_v10.hs_cond_dimension cd
                        WHERE upper(cd.cond_type) = 'TYPE_CALC' AND cd.id = rec_sctr_master.calculation_type
                    ) calculation_type,
                       (
                        SELECT cd.id
                        FROM insis_gen_v10.hs_cond_dimension cd
                        WHERE upper(cd.cond_type) = 'TIPO_FACTURATION' AND cd.id = rec_sctr_master.billing_type
                    ) billing_type,
                       (
                        SELECT cd.id
                        FROM insis_gen_v10.hs_cond_dimension cd
                        WHERE upper(cd.cond_type) = 'FACTURA_POR' AND cd.id = rec_sctr_master.billing_way
                    ) billing_way,
                       (
                        SELECT pc.client_id
                        FROM insis_cust.intrf_lpv_people_ids lc,
                             insis_people_v10.p_clients pc
                        WHERE lc.man_id = pc.man_id
                            --ISS025-Uses INSUNIX codes     
                         AND lc.insunix_code = rec_sctr_master.policy_holder_code
                    ) client_id,
                       (
                        SELECT pa.agent_id
                        FROM insis_cust.intrf_lpv_people_ids lc,
                             insis_people_v10.p_agents pa
                        WHERE lc.man_id = pa.man_id
                            --ISS025-Uses INSUNIX codes     
                         AND lc.insunix_code = rec_sctr_master.broker_code
                    ) agent_id,
                       (
                        SELECT pa.agent_type
                        FROM insis_cust.intrf_lpv_people_ids lc,
                             insis_people_v10.p_agents pa
                        WHERE lc.man_id = pa.man_id
                            --ISS025-Uses INSUNIX codes     
                         AND lc.insunix_code = rec_sctr_master.broker_code
                    ) agent_type,
                       (
                        CASE
                            WHEN rec_sctr_master.as_is_product_code = '0' THEN 'Y'
                            ELSE
                                'N'
                        END
                    ) as_is_code
                into
                    l_validation_id,
                    l_insis_product_code_rev,
                    l_sales_channel_code_rev,
                    l_office_number_rev,
                    l_currency_code_rev,
                    l_calculation_type_rev,
                    l_billing_type_rev,
                    l_billing_way_rev,
                    l_policy_holder_code_rev,
                    l_broker_code_rev,
                    l_agent_type,
                    l_as_is_product_code_rev
                FROM dual;

            EXCEPTION
                WHEN OTHERS THEN
                    putlog(
                        pi_control_id,
                        0,
                        'sctr_wrapper|looping values.err|' || sqlerrm
                    );
                    ins_error_stg(
                        pi_control_id,
                        rec_sctr_master.stag_id,
                        'ERR',
                        'qry_validation',
                        sqlerrm,
                        pio_err
                    );
            END;

            IF rec_sctr_master.insis_product_code IS NULL
            THEN
                ins_error_stg(
                    pi_control_id,
                    rec_sctr_master.stag_id,
                    'ERR',
                    'Upload_SctrMaster_Null_Product_Code',
                    srv_error.getsrvmessage('Upload_SctrMaster_Null_Product_Code'),
                    pio_err
                );

                v_exito   := 'ERR';
            ELSE
                IF
                    l_insis_product_code_rev = 'N'
                THEN
                    ins_error_stg(
                        pi_control_id,
                        rec_sctr_master.stag_id,
                        'ERR',
                        'Upload_SctrMaster_Validation_Invalid_Product',
                        srv_error.getsrvmessage('Upload_SctrMaster_Validation_Invalid_Product'),
                        pio_err
                    );

                    v_exito   := 'ERR';
                END IF;
            END IF;

            IF rec_sctr_master.as_is_product_code IS NULL
            THEN
                ins_error_stg(
                    pi_control_id,
                    rec_sctr_master.stag_id,
                    'ERR',
                    'Upload_SctrMaster_Null_AsIs',
                    srv_error.getsrvmessage('Upload_SctrMaster_Null_AsIs'),
                    pio_err
                );

                v_exito   := 'ERR';
            ELSE
                IF l_as_is_product_code_rev IS NULL
                THEN
                    ins_error_stg(
                        pi_control_id,
                        rec_sctr_master.stag_id,
                        'ERR',
                        'Upload_SctrMaster_Validation_Invalid_AsIs',
                        srv_error.getsrvmessage('Upload_SctrMaster_Validation_Invalid_AsIs'),
                        pio_err
                    );

                    v_exito   := 'ERR';
                END IF;
            END IF;

            IF rec_sctr_master.policy_holder_code IS NULL
            THEN
                ins_error_stg(
                    pi_control_id,
                    rec_sctr_master.stag_id,
                    'ERR',
                    'Upload_SctrMaster_Null_Policy_Holder',
                    srv_error.getsrvmessage('Upload_SctrMaster_Null_Policy_Holder'),
                    pio_err
                );

                v_exito   := 'ERR';
            ELSE
                IF l_policy_holder_code_rev IS NULL
                THEN
                    ins_error_stg(
                        pi_control_id,
                        rec_sctr_master.stag_id,
                        'ERR',
                        'Upload_SctrMaster_Validation_Invalid_Pholder',
                        srv_error.getsrvmessage('Upload_SctrMaster_Validation_Invalid_Pholder'),
                        pio_err
                    );

                    v_exito   := 'ERR';
                END IF;
            END IF;

            IF rec_sctr_master.broker_code IS NULL
            THEN
                ins_error_stg(
                    pi_control_id,
                    rec_sctr_master.stag_id,
                    'ERR',
                    'Upload_SctrMaster_Null_Broker_Code',
                    srv_error.getsrvmessage('Upload_SctrMaster_Null_Broker_Code'),
                    pio_err
                );

                v_exito   := 'ERR';
            ELSE
                IF l_broker_code_rev IS NULL
                THEN
                    ins_error_stg(
                        pi_control_id,
                        rec_sctr_master.stag_id,
                        'ERR',
                        lc_val_invalid_broker_id,
                        srv_error.getsrvmessage(lc_val_invalid_broker_id,rec_sctr_master.broker_code),
                        pio_err
                    );

                    v_exito   := 'ERR';
                END IF;
            END IF;

            IF
                rec_sctr_master.sales_channel_code = 1
            THEN --BROKER
                IF
                    l_agent_type <> 5
                THEN
                    ins_error_stg(
                        pi_control_id,
                        rec_sctr_master.stag_id,
                        'ERR',
                        'Upload_SctrMaster_Validation_SalesChannel_Broker',
                        srv_error.getsrvmessage('Upload_SctrMaster_Validation_SalesChannel_Broker'),
                        pio_err
                    );

                    v_exito   := 'ERR';
                END IF;
            END IF;

            IF
                rec_sctr_master.sales_channel_code = 3
            THEN --DIRECTOS
                IF
                    l_agent_type <> 1 OR rec_sctr_master.broker_code <> 'N0000202587'
                THEN
                    ins_error_stg(
                        pi_control_id,
                        rec_sctr_master.stag_id,
                        'ERR',
                        'Upload_SctrMaster_Validation_SalesChannel_Directo',
                        srv_error.getsrvmessage('Upload_SctrMaster_Validation_SalesChannel_Directo'),
                        pio_err
                    );

                    v_exito   := 'ERR';
                END IF;
            END IF;

            IF rec_sctr_master.sales_channel_code IS NULL
            THEN
                ins_error_stg(
                    pi_control_id,
                    rec_sctr_master.stag_id,
                    'ERR',
                    'Upload_SctrMaster_Null_Sales_Channel',
                    srv_error.getsrvmessage('Upload_SctrMaster_Null_Sales_Channel'),
                    pio_err
                );

                v_exito   := 'ERR';
            ELSE
                IF l_sales_channel_code_rev IS NULL
                THEN
                    ins_error_stg(
                        pi_control_id,
                        rec_sctr_master.stag_id,
                        'ERR',
                        'Upload_SctrMaster_Validation_Invalid_SalesChannel',
                        srv_error.getsrvmessage('Upload_SctrMaster_Validation_Invalid_SalesChannel'),
                        pio_err
                    );

                    v_exito   := 'ERR';
                END IF;
            END IF;

            IF rec_sctr_master.commiss_perc IS NULL
            THEN
                ins_error_stg(
                    pi_control_id,
                    rec_sctr_master.stag_id,
                    'ERR',
                    'Upload_SctrMaster_Null_Commiss_Perc',
                    srv_error.getsrvmessage('Upload_SctrMaster_Null_Commiss_Perc'),
                    pio_err
                );

                v_exito   := 'ERR';
            END IF;

            IF rec_sctr_master.office_number IS NULL
            THEN
                ins_error_stg(
                    pi_control_id,
                    rec_sctr_master.stag_id,
                    'ERR',
                    'Upload_SctrMaster_Null_Office_No',
                    srv_error.getsrvmessage('Upload_SctrMaster_Null_Office_No'),
                    pio_err
                );

                v_exito   := 'ERR';
            ELSE
                IF l_office_number_rev IS NULL
                THEN
                    ins_error_stg(
                        pi_control_id,
                        rec_sctr_master.stag_id,
                        'ERR',
                        'Upload_SctrMaster_Validation_Invalid_OfficeNo',
                        srv_error.getsrvmessage('Upload_SctrMaster_Validation_Invalid_OfficeNo'),
                        pio_err
                    );

                    v_exito   := 'ERR';
                END IF;
            END IF;

            IF rec_sctr_master.activity_code IS NULL
            THEN
                ins_error_stg(
                    pi_control_id,
                    rec_sctr_master.stag_id,
                    'ERR',
                    'Upload_SctrMaster_Null_Activity_Code',
                    srv_error.getsrvmessage('Upload_SctrMaster_Null_Activity_Code'),
                    pio_err
                );

                v_exito   := 'ERR';
            END IF;

            --ISS028-Activity detail could be null
--            IF rec_sctr_master.activity_detail IS NULL THEN
--                ins_error_stg(pi_control_id,rec_sctr_master.stag_id,rec_sctr_master.rowseq,1,'cust_migration.FA_CUST_MIGR_SCTR.sctr_wrapper',
--                                 'Upload_SctrMaster_Null_Activity_Detail',pio_err);
--
--                v_exito := 'ERR';
--            END IF;

            IF rec_sctr_master.section_code IS NULL
            THEN
                ins_error_stg(
                    pi_control_id,
                    rec_sctr_master.stag_id,
                    'ERR',
                    'Upload_SctrMaster_Null_Section_Code',
                    srv_error.getsrvmessage('Upload_SctrMaster_Null_Section_Code'),
                    pio_err
                );

                v_exito   := 'ERR';
            END IF;

            IF rec_sctr_master.currency_code IS NULL
            THEN
                ins_error_stg(
                    pi_control_id,
                    rec_sctr_master.stag_id,
                    'ERR',
                    'Upload_SctrMaster_Null_Currency_Code',
                    srv_error.getsrvmessage('Upload_SctrMaster_Null_Currency_Code'),
                    pio_err
                );

                v_exito   := 'ERR';
            ELSE
                IF l_currency_code_rev IS NULL
                THEN
                    ins_error_stg(
                        pi_control_id,
                        rec_sctr_master.stag_id,
                        'ERR',
                        'Upload_SctrMaster_Validation_Invalid_Currency',
                        srv_error.getsrvmessage('Upload_SctrMaster_Validation_Invalid_Currencypio_err'),
                        pio_err
                    );

                    v_exito   := 'ERR';
                END IF;
            END IF;

            IF rec_sctr_master.begin_date IS NULL
            THEN
                ins_error_stg(
                    pi_control_id,
                    rec_sctr_master.stag_id,
                    'ERR',
                    'Upload_SctrMaster_Null_Begin_Date',
                    srv_error.getsrvmessage('Upload_SctrMaster_Null_Begin_Date'),
                    pio_err
                );

                v_exito   := 'ERR';
            END IF;

            IF rec_sctr_master.end_date IS NULL
            THEN
                ins_error_stg(
                    pi_control_id,
                    rec_sctr_master.stag_id,
                    'ERR',
                    'Upload_SctrMaster_Null_End_Date',
                    srv_error.getsrvmessage('Upload_SctrMaster_Null_End_Date'),
                    pio_err
                );

                v_exito   := 'ERR';
            END IF;

            IF rec_sctr_master.date_covered IS NULL
            THEN
                ins_error_stg(
                    pi_control_id,
                    rec_sctr_master.stag_id,
                    'ERR',
                    'Upload_SctrMaster_Null_Date_Covered',
                    srv_error.getsrvmessage('Upload_SctrMaster_Null_Date_Covered'),
                    pio_err
                );

                v_exito   := 'ERR';
            END IF;

            IF rec_sctr_master.prem_period_code IS NULL
            THEN
                ins_error_stg(
                    pi_control_id,
                    rec_sctr_master.stag_id,
                    'ERR',
                    'Upload_SctrMaster_Null_Prem_Period_Code',
                    srv_error.getsrvmessage('Upload_SctrMaster_Null_Prem_Period_Code'),
                    pio_err
                );

                v_exito   := 'ERR';
            END IF;

            IF rec_sctr_master.min_prem_issue IS NULL
            THEN
                ins_error_stg(
                    pi_control_id,
                    rec_sctr_master.stag_id,
                    'ERR',
                    'Upload_SctrMaster_Null_Min_Prem_Issue',
                    srv_error.getsrvmessage('Upload_SctrMaster_Null_Min_Prem_Issue'),
                    pio_err
                );

                v_exito   := 'ERR';
            END IF;

            IF rec_sctr_master.min_prem_attach IS NULL
            THEN
                ins_error_stg(
                    pi_control_id,
                    rec_sctr_master.stag_id,
                    'ERR',
                    'Upload_SctrMaster_Null_Min_Prem_Attach',
                    srv_error.getsrvmessage('Upload_SctrMaster_Null_Min_Prem_Attach'),
                    pio_err
                );

                v_exito   := 'ERR';
            END IF;

            IF rec_sctr_master.iss_exp_percentage IS NULL
            THEN
                ins_error_stg(
                    pi_control_id,
                    rec_sctr_master.stag_id,
                    'ERR',
                    'Upload_SctrMaster_Null_Iss_Exp_Percentage',
                    srv_error.getsrvmessage('Upload_SctrMaster_Null_Iss_Exp_Percentage'),
                    pio_err
                );

                v_exito   := 'ERR';
            END IF;

            IF rec_sctr_master.min_iss_expenses IS NULL
            THEN
                ins_error_stg(
                    pi_control_id,
                    rec_sctr_master.stag_id,
                    'ERR',
                    'Upload_SctrMaster_Null_Min_Iss_Exp_Amount',
                    srv_error.getsrvmessage('Upload_SctrMaster_Null_Min_Iss_Exp_Amount'),
                    pio_err
                );

                v_exito   := 'ERR';
            END IF;

            IF rec_sctr_master.calculation_type IS NULL
            THEN
                ins_error_stg(
                    pi_control_id,
                    rec_sctr_master.stag_id,
                    'ERR',
                    'Upload_SctrMaster_Null_Calculation_Type',
                    srv_error.getsrvmessage('Upload_SctrMaster_Null_Calculation_Type'),
                    pio_err
                );

                v_exito   := 'ERR';
            ELSE
                IF
                    rec_sctr_master.calculation_type IS NOT NULL AND l_calculation_type_rev IS NULL
                THEN
                    ins_error_stg(
                        pi_control_id,
                        rec_sctr_master.stag_id,
                        'ERR',
                        'Upload_SctrMaster_Validation_Invalid_CalcType',
                        srv_error.getsrvmessage('Upload_SctrMaster_Validation_Invalid_CalcType'),
                        pio_err
                    );

                    v_exito   := 'ERR';
                END IF;
            END IF;

            IF rec_sctr_master.billing_type IS NULL
            THEN
                ins_error_stg(
                    pi_control_id,
                    rec_sctr_master.stag_id,
                    'ERR',
                    'Upload_SctrMaster_Null_Billing_Type',
                    srv_error.getsrvmessage('Upload_SctrMaster_Null_Billing_Type'),
                    pio_err
                );

                v_exito   := 'ERR';
            ELSE
                IF l_billing_type_rev IS NULL
                THEN
                    ins_error_stg(
                        pi_control_id,
                        rec_sctr_master.stag_id,
                        'ERR',
                        'Upload_SctrMaster_Validation_Invalid_BillingType',
                        srv_error.getsrvmessage('Upload_SctrMaster_Validation_Invalid_BillingType'),
                        pio_err
                    );

                    v_exito   := 'ERR';
                END IF;

                IF rec_sctr_master.billing_type IN (
                        '2','3'
                    )
                THEN
                    IF
                        months_between(
                            tdate(rec_sctr_master.end_date),
                            tdate(rec_sctr_master.begin_date)
                        ) <> 1
                    THEN
                        ins_error_stg(
                            pi_control_id,
                            rec_sctr_master.stag_id,
                            'ERR',
                            'Upload_SctrMaster_Validation_Monthly_Duration',
                            srv_error.getsrvmessage('Upload_SctrMaster_Validation_Monthly_Duration'),
                            pio_err
                        );

                        v_exito   := 'ERR';
                    END IF;

                END IF;

            END IF;

            IF rec_sctr_master.billing_way IS NULL
            THEN
                ins_error_stg(
                    pi_control_id,
                    rec_sctr_master.stag_id,
                    'ERR',
                    'Upload_SctrMaster_Null_Billing_Way',
                    srv_error.getsrvmessage('Upload_SctrMaster_Null_Billing_Way'),
                    pio_err
                );

                v_exito   := 'ERR';
            ELSE
                IF l_billing_way_rev IS NULL
                THEN
                    ins_error_stg(
                        pi_control_id,
                        rec_sctr_master.stag_id,
                        'ERR',
                        'Upload_SctrMaster_Validation_Invalid_BillingWay',
                        srv_error.getsrvmessage('Upload_SctrMaster_Validation_Invalid_BillingWay'),
                        pio_err
                    );

                    v_exito   := 'ERR';
                END IF;
            END IF;

            IF rec_sctr_master.warranty_clause_flag IS NULL
            THEN
                ins_error_stg(
                    pi_control_id,
                    rec_sctr_master.stag_id,
                    'ERR',
                    'Upload_SctrMaster_Null_Warranty_Clause_Flag',
                    srv_error.getsrvmessage('Upload_SctrMaster_Null_Warranty_Clause_Flag'),
                    pio_err
                );

                v_exito   := 'ERR';
            END IF;

            IF rec_sctr_master.spec_pen_clause_flag IS NULL
            THEN
                ins_error_stg(
                    pi_control_id,
                    rec_sctr_master.stag_id,
                    'ERR',
                    'Upload_SctrMaster_Null_Spec_Pen_Clause_Flag',
                    srv_error.getsrvmessage('Upload_SctrMaster_Null_Spec_Pen_Clause_Flag'),
                    pio_err
                );

                v_exito   := 'ERR';
            END IF;

--            IF rec_sctr_master.spec_pen_clause_detail IS NULL THEN
--                ins_error_stg(pi_control_id,rec_sctr_master.stag_id,rec_sctr_master.rowseq,1,'cust_migration.FA_CUST_MIGR_SCTR.sctr_wrapper',
--                                 'Upload_SctrMaster_Null_Spec_Pen_Clause_Detail',pio_err);
--
--                v_exito := 'ERR';
--            END IF;

            IF rec_sctr_master.gratuity_flag IS NULL
            THEN
                ins_error_stg(
                    pi_control_id,
                    rec_sctr_master.stag_id,
                    'ERR',
                    'Upload_SctrMaster_Null_Spec_Gratuity_Flag',
                    srv_error.getsrvmessage('Upload_SctrMaster_Null_Spec_Gratuity_Flag'),
                    pio_err
                );

                v_exito   := 'ERR';
            END IF;

            IF rec_sctr_master.consortium_flag IS NULL
            THEN
                ins_error_stg(
                    pi_control_id,
                    rec_sctr_master.stag_id,
                    'ERR',
                    'Upload_SctrMaster_Null_Consortium_Flag',
                    srv_error.getsrvmessage('Upload_SctrMaster_Null_Product_Code'),
                    pio_err
                );

                v_exito   := 'ERR';
            END IF;

            IF rec_sctr_master.elec_pol_flag IS NULL
            THEN
                ins_error_stg(
                    pi_control_id,
                    rec_sctr_master.stag_id,
                    'ERR',
                    'Upload_SctrMaster_Null_Elec_Pol_Flag',
                    srv_error.getsrvmessage('Upload_SctrMaster_Null_Elec_Pol_Flag'),
                    pio_err
                );

                v_exito   := 'ERR';
            END IF;

            IF rec_sctr_master.tender_flag IS NULL
            THEN
                ins_error_stg(
                    pi_control_id,
                    rec_sctr_master.stag_id,
                    'ERR',
                    'Upload_SctrMaster_Null_Tender_Flag',
                    srv_error.getsrvmessage('Upload_SctrMaster_Null_Tender_Flag'),
                    pio_err
                );

                v_exito   := 'ERR';
            END IF;
            
            IF
                  rec_sctr_master.prem_period_code > 0 and add_months(
                        tdate(rec_sctr_master.begin_date),
                        rec_sctr_master.prem_period_code
                  ) > tdate(rec_sctr_master.end_date)
            then
                  ins_error_stg(
                        pi_control_id,
                        rec_sctr_master.stag_id,
                        'ERR',
                        'Master prem_period_code',
                        'Master Premium Period > Policy duration',
                        pio_err
                  );
                  v_exito   := 'ERR';
            end if;
            
            IF
                v_exito = 'OK'
            THEN
                UPDATE cust_migration.fa_migr_sctr_stg
                    SET
                        att_status = lc_stat_rec_valid
                WHERE control_id = pi_control_id AND stag_id = rec_sctr_master.stag_id;

            ELSE
                UPDATE cust_migration.fa_migr_sctr_stg
                    SET
                        att_status = lc_stat_rec_error
                WHERE control_id = pi_control_id AND stag_id = rec_sctr_master.stag_id;

            END IF;

        END LOOP;

        COMMIT;
        putlog(pi_control_id,0,'sctr_wrapper|defining jobs');
        OPEN c_job_size(l_number_of_jobs);
        FETCH c_job_size INTO l_job_size;
        CLOSE c_job_size;
        IF
            l_job_size = 0
        THEN
            l_job_size   := 1;
        END IF;
        l_task_name             := pi_control_id || '_';
        newjobarr               := sys.job_definition_array ();
        putlog(pi_control_id,0,'sctr_wrapper|starting jobs');
        FOR c_st IN (
            WITH a AS (
                SELECT stag_id,
                       ROW_NUMBER() OVER(PARTITION BY
                        control_id
                        ORDER BY
                            stag_id
                    ) AS rownum_
                FROM cust_migration.fa_migr_sctr_stg
                WHERE control_id = pi_control_id
                    --ISS030-Job to process only valid records
                 AND att_status = lc_stat_rec_valid
            ),b AS (
                SELECT stag_id,
                       trunc(
                        CASE
                            WHEN mod(rownum_,l_job_size) = 0 THEN
                                rownum_ / l_job_size
                            ELSE
                                rownum_ / l_job_size + 1
                        END
                    ) AS page_num
                FROM a
            ),c AS (
                SELECT MIN(stag_id) AS fv,
                       MAX(stag_id) AS lv,
                       page_num
                FROM b
                GROUP BY
                    page_num
            ) SELECT fv,
                   lv,
                   c.page_num
            FROM c
            ORDER BY c.page_num
        ) LOOP
            l_sql_stmt           := 'BEGIN cust_migration.FA_CUST_MIGR_SCTR.sctr_job_proc (' ||pi_control_id ||',' ||c_st.fv ||',' ||c_st.lv ||',' ||pi_file_id ||',' ||chr(39) ||pi_file_name ||chr(39) ||',' ||chr(39) ||pi_poller_name ||chr(39) ||'); END;';

            putlog(
                pi_control_id,
                0,
                'sctr_wrapper|job[' ||c_st.page_num ||']: ' ||l_sql_stmt
            );

            l_n_job              := l_n_job + 1;
            l_job_name           := l_task_name || l_n_job;
            newjobarr.extend;
            newjob               := sys.job_definition(
                job_name => '"' || l_job_name || '"',
                job_style => 'REGULAR',
                number_of_arguments => 0,
                job_type => 'PLSQL_BLOCK',
                job_action => l_sql_stmt,
                start_date => SYSDATE,
                enabled => true,
                auto_drop => true,
                comments => 'one-time job'
            );

            newjobarr(l_n_job)   := newjob;
        END LOOP;

        putlog(pi_control_id,0,'sctr_wrapper|waiting jobs...');
        dbms_scheduler.create_jobs(newjobarr,'TRANSACTIONAL'); --TRANSACTIONAL STOP_ON_FIRST_ERROR  ABSORB_ERRORS
        dbms_lock.sleep(1);
        WHILE ( true ) LOOP
            SELECT COUNT(1)
            INTO
                l_jobs_running
            FROM all_scheduler_running_jobs
            WHERE job_name LIKE '%' || l_task_name || '%';

            IF
                l_jobs_running > 0
            THEN
                dbms_lock.sleep(1);
            ELSE
                putlog(pi_control_id,0,'sctr_wrapper|end_jobs');
                EXIT;
            END IF;

        END LOOP;

        putlog(pi_control_id,0,'sctr_wrapper|checking_errors...');
        SELECT COUNT(*)
        INTO
            v_count_errors
        FROM cust_migration.fa_migr_sctr_err
        WHERE control_id = pi_control_id AND stag_id > 0 AND errseq > 0;

        putlog(
            pi_control_id,
            0,
            'sctr_wrapper|count:' || v_count_errors
        );
        IF
            v_count_errors > 0
        THEN
            insis_cust_lpv.sys_schema_utils.update_poller_process_status(pi_control_id,'ERROR');
        ELSE
            insis_cust_lpv.sys_schema_utils.update_poller_process_status(pi_control_id,'SUCCESS');
        END IF;
        
        --creates record for report process

        INSERT INTO cust_migration.fa_migr_sctr_err (
            control_id,
            stag_id,
            errseq,
            errtype,
            errcode,
            errmess
        ) VALUES (
            pi_control_id,
            0,
            0,
            'REP',
            NULL,
            '--Record ready for report--'
        );

        COMMIT;
        putlog(pi_control_id,0,'sctr_wrapper|end');
    EXCEPTION
        WHEN OTHERS THEN
            putlog(
                pi_control_id,
                0,
                'sctr_wrapper|end_error|' || sqlerrm
            );
            ins_error_stg(
                pi_control_id,
                0,
                'ERR',
                'sctr_wrapper',
                sqlerrm,
                pio_err
            );
    END sctr_wrapper;

    PROCEDURE sctr_job_proc (
        pi_control_id IN NUMBER,
        pi_stag_init IN NUMBER,
        pi_stag_end IN NUMBER,
        pi_file_id IN NUMBER,
        pi_file_name IN VARCHAR2,
        pi_poller_name IN VARCHAR2
    ) IS

        pio_err srverr;
        v_errm VARCHAR2(4000);
        v_file_id NUMBER;
        v_stat_poller VARCHAR(500);
        pio_errmsg srverr;
    BEGIN
        v_stat_poller   := NULL;
        l_log_proc      := pi_control_id;
        putlog(
            pi_control_id,
            0,
            'sctr_job_proc|start|' ||pi_control_id ||',' ||pi_stag_init
        );
        SELECT status
        INTO
            v_stat_poller
        FROM insis_cust_lpv.sys_poller_process_ctrl
        WHERE sys_poller_process_ctrl_id = pi_control_id;

        IF
            v_stat_poller <> 'ERROR'
        THEN
            FOR rec_sctr_master_record IN (
                SELECT *
                FROM cust_migration.fa_migr_sctr_stg
                WHERE control_id = pi_control_id AND stag_id BETWEEN pi_stag_init AND pi_stag_end AND att_status = lc_stat_rec_valid
            ) LOOP
                SAVEPOINT process_data_sp;
                pio_errmsg   := NEW srverr ();
                sctr_record_proc(rec_sctr_master_record,pio_errmsg);
                IF
                    insis_sys_v10.srv_error.rqstatus(pio_errmsg)
                THEN
--                    UPDATE cust_migration.FA_MIGR_SCTR_STG
--                    SET
--                        att_status = lc_stat_rec_valid
--                    WHERE
--                          control_id = pi_control_id
--                      and stag_id = rec_sctr_master_record.stag_id;
                    COMMIT;
                ELSE
                    ROLLBACK TO process_data_sp;
                    UPDATE cust_migration.fa_migr_sctr_stg
                        SET
                            att_status = lc_stat_rec_error
                    WHERE control_id = pi_control_id AND stag_id = rec_sctr_master_record.stag_id;

                    COMMIT;
                END IF;

            END LOOP;
        ELSE
            insis_cust_lpv.sys_schema_utils.update_poller_process_status(pi_control_id,'ERROR');
        END IF;

        putlog(pi_control_id,0,'sctr_job_proc|end|');
    EXCEPTION
        WHEN OTHERS THEN
            putlog(pi_control_id,0,'sctr_job_proc|end-err');
    END sctr_job_proc;

    PROCEDURE sctr_record_proc (
        pi_fa_sctr_row IN cust_migration.fa_migr_sctr_stg%rowtype,
        pio_errmsg IN OUT srverr
    ) IS

        v_code VARCHAR(4000);
        v_errm VARCHAR(4000);
        l_outcontext srvcontext;
        l_client_id insis_people_v10.p_clients.client_id%TYPE;
        l_office_id insis_people_v10.p_offices.office_id%TYPE;
        l_engagement_id insis_gen_v10.policy_engagement.engagement_id%TYPE;
        l_agent_id insis_people_v10.p_agents.agent_id%TYPE;
        l_agent_id_directos insis_people_v10.p_agents.agent_id%TYPE;
        l_agent_type insis_people_v10.p_agents.agent_type%TYPE;
        l_internal_agent_id insis_people_v10.p_agents.agent_id%TYPE;
        l_master_policy_id insis_gen_v10.policy.policy_id%TYPE;
        calc_duration NUMBER;
        calc_dimension VARCHAR2(1);
        TYPE list_of_workers_cat IS
            TABLE OF VARCHAR2(100);
        list_of_workers list_of_workers_cat;
        l_object_type NUMBER;
        l_parent_obj_type NUMBER;
        l_parent_obj_type_aux NUMBER;
        count_parent_obj_type NUMBER;
        l_description VARCHAR2(100);
        l_parent_ins_obj_id NUMBER;
        l_ins_obj_id NUMBER;
        l_procedure_result VARCHAR2(100);
        l_tariff_percent NUMBER;
        l_begin_date DATE;
        l_end_date DATE;
        l_date_covered DATE;
        l_result BOOLEAN;
    BEGIN
        l_log_proc                                                                      := pi_fa_sctr_row.control_id ||'-' ||pi_fa_sctr_row.stag_id;
        putlog(
            pi_fa_sctr_row.control_id,
            pi_fa_sctr_row.stag_id,
            'sctr_record_proc|start|' || pi_fa_sctr_row.stag_id
        );
        EXECUTE IMMEDIATE 'alter session set NLS_NUMERIC_CHARACTERS = ''.,''';
        insis_sys_v10.insis_context.prepare_session(
            'GEN',
            NULL,
            'insis_gen_v10',
            'InsisStaff',
            NULL,
            NULL
        );

        --Inicializar variable de contexto     

        l_outcontext                                                                    := srvcontext ();
        putlog(
            pi_fa_sctr_row.control_id,
            pi_fa_sctr_row.stag_id,
            'select client_id'
        );
        BEGIN
            SELECT client_id
            INTO
                l_client_id
            FROM insis_people_v10.p_clients
            WHERE man_id = (
                    SELECT itf.man_id
                    FROM insis_cust.intrf_lpv_people_ids itf
                    WHERE
                        --ISS025-Uses INSUNIX codes 
                     itf.insunix_code = pi_fa_sctr_row.policy_holder_code
                );

        EXCEPTION
            WHEN OTHERS THEN
                ins_error_stg(
                    pi_fa_sctr_row.control_id,
                    pi_fa_sctr_row.stag_id,
                    'ERR',
                    'sctr_record_proc-client_id',
                    sqlerrm,
                    pio_errmsg
                );

                return;
        END;

        putlog(
            pi_fa_sctr_row.control_id,
            pi_fa_sctr_row.stag_id,
            'select agent_id'
        );
        BEGIN
            SELECT agent_id,
                   agent_type
            INTO
                l_agent_id,l_agent_type
            FROM insis_people_v10.p_agents
            WHERE man_id = (
                    SELECT itf.man_id
                    FROM insis_cust.intrf_lpv_people_ids itf
                    WHERE
                        --ISS025-Uses INSUNIX codes 
                     itf.insunix_code = pi_fa_sctr_row.broker_code
                );

        EXCEPTION
            WHEN OTHERS THEN
                ins_error_stg(
                    pi_fa_sctr_row.control_id,
                    pi_fa_sctr_row.stag_id,
                    'ERR',
                    'sctr_record_proc-agent_id',
                    sqlerrm,
                    pio_errmsg
                );

                return;
        END;

        putlog(
            pi_fa_sctr_row.control_id,
            pi_fa_sctr_row.stag_id,
            'select office_id'
        );
        BEGIN
            SELECT office_id
            INTO
                l_office_id
            FROM insis_people_v10.p_offices
            WHERE office_no =
                    CASE
                        WHEN length(pi_fa_sctr_row.office_number) = 1 THEN lpad(
                            pi_fa_sctr_row.office_number,
                            2,
                            '0'
                        )
                        ELSE
                            TO_CHAR(pi_fa_sctr_row.office_number)
                    END;

        EXCEPTION
            WHEN OTHERS THEN
                ins_error_stg(
                    pi_fa_sctr_row.control_id,
                    pi_fa_sctr_row.stag_id,
                    'ERR',
                    'sctr_record_proc-office_id',
                    sqlerrm,
                    pio_errmsg
                );

                return;
        END;

        putlog(
            pi_fa_sctr_row.control_id,
            pi_fa_sctr_row.stag_id,
            '--CREATE_ENGAGEMENT'
        ); 
                                          
        --================================================================================================
        --PREPARE INFORMATION FOR CREATE_ENGAGEMENT EVENT
        --================================================================================================         
        insis_sys_v10.srv_context.setcontextattrnumber(
            l_outcontext,
            'ENGAGEMENT_ID',
            insis_sys_v10.srv_context.integers_format,
            NULL
        );

        insis_sys_v10.srv_context.setcontextattrnumber(
            l_outcontext,
            'CLIENT_ID',
            insis_sys_v10.srv_context.integers_format,
            l_client_id
        );

        insis_sys_v10.srv_context.setcontextattrchar(
            l_outcontext,
            'ENGAGEMENT_STAGE',
            insis_gen_v10.gvar_pas.at_appl
        );
        insis_sys_v10.srv_context.setcontextattrchar(
            l_outcontext,
            'ENGAGEMENT_TYPE',
            insis_gen_v10.gvar_pas.eng_type_engagement
        );
            
        --================================================================================================
        -- CREATE_ENGAGEMENT
        -- Output parameter : ENGAGEMENT_ID
        --================================================================================================
        insis_sys_v10.srv_events.sysevent(
            'CREATE_ENGAGEMENT',
            l_outcontext,
            l_outcontext,
            pio_errmsg
        );
        IF
            NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg)
        THEN
            putlog(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                '--CREATE_ENGAGEMENT.err'
            );
            ins_error_stg(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                'ERR',
                'CREATE_ENGAGEMENT',
                srv_error.errcollection2string(pio_errmsg),
                pio_errmsg
            );

            return;
        END IF;

        insis_sys_v10.srv_context.getcontextattrnumber(l_outcontext,'ENGAGEMENT_ID',l_engagement_id);
        
        
        --================================================================================================
        --PREPARE INFORMATION FOR CREATE_ENG_POLICY EVENT
        --================================================================================================   
        insis_sys_v10.srv_context.setcontextattrnumber(
            l_outcontext,
            'ENGAGEMENT_ID',
            insis_sys_v10.srv_context.integers_format,
            l_engagement_id
        );

        insis_sys_v10.srv_context.setcontextattrnumber(
            l_outcontext,
            'INSR_TYPE',
            insis_sys_v10.srv_context.integers_format,
            pi_fa_sctr_row.insis_product_code
        );

        insis_sys_v10.srv_context.setcontextattrchar(
            l_outcontext,
            'POLICY_TYPE',
            insis_gen_v10.gvar_pas.engpoltype_master
        );
        insis_sys_v10.srv_context.setcontextattrnumber(
            l_outcontext,
            'POLICY_ID_ORG',
            insis_sys_v10.srv_context.integers_format,
            NULL
        );
        --insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext,'AGENT_ID',insis_sys_v10.srv_context.integers_format,l_internal_agent_id);

        putlog(
            pi_fa_sctr_row.control_id,
            pi_fa_sctr_row.stag_id,
            'select agent_id 1412'
        );
        IF pi_fa_sctr_row.att_int_agent_id IS NULL
        THEN
            IF
                l_agent_type = lc_agent_type_broker
            THEN
                BEGIN
                    SELECT agent_id
                    INTO
                        l_agent_id_directos
                    FROM insis_people_v10.p_agents
                    WHERE agent_no = '1412';

                EXCEPTION
                    WHEN OTHERS THEN
                        ins_error_stg(
                            pi_fa_sctr_row.control_id,
                            pi_fa_sctr_row.stag_id,
                            'ERR',
                            'SELECT_AGENT_ID_1412',
                            sqlerrm,
                            pio_errmsg
                        );

                        return;
                END;

                insis_sys_v10.srv_context.setcontextattrnumber(
                    l_outcontext,
                    'AGENT_ID',
                    insis_sys_v10.srv_context.integers_format,
                    l_agent_id_directos
                );--DIRECTOS            

            ELSE
                insis_sys_v10.srv_context.setcontextattrnumber(
                    l_outcontext,
                    'AGENT_ID',
                    insis_sys_v10.srv_context.integers_format,
                    l_agent_id
                );
            END IF;
        ELSE
            insis_sys_v10.srv_context.setcontextattrnumber(
                l_outcontext,
                'AGENT_ID',
                insis_sys_v10.srv_context.integers_format,
                pi_fa_sctr_row.att_int_agent_id
            );
        END IF;

        insis_sys_v10.srv_context.setcontextattrnumber(
            l_outcontext,
            'POLICY_STAGE',
            insis_sys_v10.srv_context.integers_format,
            insis_gen_v10.gvar_pas.define_applprep_state
        );

        --================================================================================================
        -- CREATE_ENG_POLICY
        -- Output parameter : POLICY_ID
        --================================================================================================    

        putlog(
            pi_fa_sctr_row.control_id,
            pi_fa_sctr_row.stag_id,
            '--CREATE_ENG_POLICY'
        );
        insis_sys_v10.srv_events.sysevent(
            'CREATE_ENG_POLICY',
            l_outcontext,
            l_outcontext,
            pio_errmsg
        );
        IF
            NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg)
        THEN
            putlog(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                '--CREATE_ENGAGEMENT.err'
            );
            ins_error_stg(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                'ERR',
                'CREATE_ENG_POLICY',
                srv_error.errcollection2string(pio_errmsg),
                pio_errmsg
            );

            return;
        END IF;

        insis_sys_v10.srv_context.getcontextattrnumber(l_outcontext,'POLICY_ID',l_master_policy_id);
        putlog(
            pi_fa_sctr_row.control_id,
            pi_fa_sctr_row.stag_id,
            'l_master_policy_id: ' || l_master_policy_id
        );
        
--        putlogcontext(pi_fa_sctr_row.control_id,l_outcontext);
        
        --todo expirar 1 seg antes de siguiente dia
        l_begin_date                                                                    := tdate(pi_fa_sctr_row.begin_date); --+ 0.5;
        l_end_date                                                                      := tdate(pi_fa_sctr_row.end_date); -- + 0.5 - 1/24/60/60;
        l_date_covered                                                                  := tdate(pi_fa_sctr_row.date_covered);
        IF pi_fa_sctr_row.econo_group_code IS NOT NULL
        THEN
--            putlog(pi_fa_sctr_row.control_id,pi_fa_sctr_row.stag_id,'--INS_POLICY_BENEFIT_GROUPS');
            insis_sys_v10.srv_context.setcontextattrchar(l_outcontext,'BENEFIT_GROUP_ID',NULL);
    --        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext,'ENGAGEMENT_ID',insis_sys_v10.srv_context.integers_format,l_engagement_id); --already loaded
    --        insis_sys_v10.srv_context.setcontextattrnumber(l_outcontext,'POLICY_ID',insis_sys_v10.srv_context.integers_format,l_master_policy_id); --already loaded
            insis_sys_v10.srv_context.setcontextattrnumber(
                l_outcontext,
                'ANNEX_ID',
                insis_sys_v10.srv_context.integers_format,
                insis_gen_v10.gvar_pas.def_annx_id
            );

            insis_sys_v10.srv_context.setcontextattrchar(l_outcontext,'GROUP_TYPE',lc_group_type_def);
            insis_sys_v10.srv_context.setcontextattrchar(
                l_outcontext,
                'GROUP_CODE',
                pi_fa_sctr_row.econo_group_code
            );
            insis_sys_v10.srv_context.setcontextattrdate(
                l_outcontext,
                'VALID_FROM',
                insis_sys_v10.srv_context.date_format,
                l_begin_date
            );

            insis_sys_v10.srv_context.setcontextattrdate(
                l_outcontext,
                'VALID_TO',
                insis_sys_v10.srv_context.date_format,
                l_end_date
            );

            insis_sys_v10.srv_context.setcontextattrdate(
                l_outcontext,
                'REGISTRATION_DATE',
                insis_sys_v10.srv_context.date_format,
                l_begin_date
            );

            insis_sys_v10.srv_context.setcontextattrchar(l_outcontext,'USERNAME',lc_policy_user);
            insis_sys_v10.srv_events.sysevent(
                'INS_POLICY_BENEFIT_GROUPS',
                l_outcontext,
                l_outcontext,
                pio_errmsg
            );
            IF
                NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg)
            THEN
                putlog(
                    pi_fa_sctr_row.control_id,
                    pi_fa_sctr_row.stag_id,
                    'ins_policy_benefit_groups.err:' ||srv_error.errcollection2string(pio_errmsg)
                );

                return;
            END IF;

        END IF;

        IF
            l_agent_type = lc_agent_type_broker
        THEN
            putlog(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                '--INS_POLICY_AGENTS.ASES'
            );
            insis_sys_v10.srv_context.setcontextattrnumber(
                l_outcontext,
                'POLICY_AGENT_ID',
                insis_sys_v10.srv_context.integers_format,
                NULL
            );

            insis_sys_v10.srv_context.setcontextattrnumber(
                l_outcontext,
                'AGENT_ID',
                insis_sys_v10.srv_context.integers_format,
                l_agent_id
            );

            insis_sys_v10.srv_context.setcontextattrnumber(
                l_outcontext,
                'ANNEX_ID',
                insis_sys_v10.srv_context.integers_format,
                insis_gen_v10.gvar_pas.def_annx_id
            );

            insis_sys_v10.srv_context.setcontextattrchar(l_outcontext,'AGENT_ROLE','ASES');
            insis_sys_v10.srv_context.setcontextattrnumber(
                l_outcontext,
                'COMM_SHARE',
                insis_sys_v10.srv_context.integers_format,
                100
            );

            insis_sys_v10.srv_context.setcontextattrdate(
                l_outcontext,
                'VALID_FROM',
                insis_sys_v10.srv_context.date_format,
                l_begin_date
            );

            insis_sys_v10.srv_events.sysevent(
                'INS_POLICY_AGENTS',
                l_outcontext,
                l_outcontext,
                pio_errmsg
            );
            IF
                NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg)
            THEN
                putlog(
                    pi_fa_sctr_row.control_id,
                    pi_fa_sctr_row.stag_id,
                    '--INS_POLICY_AGENTS.err'
                );
                ins_error_stg(
                    pi_fa_sctr_row.control_id,
                    pi_fa_sctr_row.stag_id,
                    'ERR',
                    'INS_POLICY_AGENTS',
                    srv_error.errcollection2string(pio_errmsg),
                    pio_errmsg
                );

                return;
            END IF;

            putlog(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                '--INS_POLICY_AGENTS.ASESVAT'
            );
            insis_sys_v10.srv_context.setcontextattrnumber(
                l_outcontext,
                'POLICY_AGENT_ID',
                insis_sys_v10.srv_context.integers_format,
                NULL
            );

            insis_sys_v10.srv_context.setcontextattrchar(l_outcontext,'AGENT_ROLE','ASESVAT');
            insis_sys_v10.srv_context.setcontextattrnumber(
                l_outcontext,
                'COMM_SHARE',
                insis_sys_v10.srv_context.integers_format,
                100
            );

            insis_sys_v10.srv_events.sysevent(
                'INS_POLICY_AGENTS',
                l_outcontext,
                l_outcontext,
                pio_errmsg
            );
            IF
                NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg)
            THEN
                putlog(
                    pi_fa_sctr_row.control_id,
                    pi_fa_sctr_row.stag_id,
                    '--INS_POLICY_AGENTS.asesvat.err'
                );
                ins_error_stg(
                    pi_fa_sctr_row.control_id,
                    pi_fa_sctr_row.stag_id,
                    'ERR',
                    'INS_POLICY_AGENTS.asesvat',
                    srv_error.errcollection2string(pio_errmsg),
                    pio_errmsg
                );

                return;
            END IF;

        END IF;

        putlog(
            pi_fa_sctr_row.control_id,
            pi_fa_sctr_row.stag_id,
            '--calcduration:' ||pi_fa_sctr_row.prem_period_code ||',' ||l_begin_date ||'-' ||l_end_date
        );
--        insis_gen_v10.pol_values.calcduration(l_begin_date,l_end_date,pi_fa_sctr_row.insis_product_code,calc_duration,calc_dimension);
--        insis_gen_v10.pol_values.CalcDuration_YMD(l_begin_date,l_end_date,pi_fa_sctr_row.insis_product_code,calc_duration,calc_dimension);
--        pol_ps_cons.covobjduration    
    
        --For special period,duration is set to days
        --ISS019-New code for Special Prem Period

        IF
            pi_fa_sctr_row.prem_period_code = 0
        THEN
            calc_dimension   := gvar_pas.dur_dim_d;
            calc_duration    := l_end_date - l_begin_date;
        ELSE
            insis_gen_v10.pol_values.calcduration(
                l_begin_date,
                l_end_date,
                pi_fa_sctr_row.insis_product_code,
                calc_duration,
                calc_dimension
            );
        END IF;

        putlog(
            pi_fa_sctr_row.control_id,
            pi_fa_sctr_row.stag_id,
            'update policy:' ||calc_duration ||',' ||calc_dimension
        );
        --todo: usa ojbect type

        UPDATE insis_gen_v10.policy
            SET
                insr_begin = l_begin_date,
                insr_end = l_end_date,
                date_given = l_begin_date,
                date_covered = l_date_covered,
                conclusion_date = l_begin_date,
                insr_duration = calc_duration,
                dur_dimension = calc_dimension,
                payment_duration = calc_duration,
                payment_dur_dim = calc_dimension,
                attr3 = pi_fa_sctr_row.sales_channel_code,
                attr4 = l_office_id,
                attr5 = pi_fa_sctr_row.prem_period_code,
                payment_type = 'S',--single premium
                username = lc_policy_user
        WHERE policy_id = l_master_policy_id;

    
        --================================================================================================
        -- Updating policy_engagement_billing
        --================================================================================================        
        
        --fill policyengagementbilling structure

        insis_gen_v10.srv_engagement_ds.get_policyengbillingbypolicy(l_outcontext,l_outcontext,pio_errmsg);
        IF
            NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg)
        THEN
            putlog(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                '--INS_POLICY_AGENTS.asesvat.err'
            );
            ins_error_stg(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                'ERR',
                'INS_POLICY_AGENTS.asesvat',
                srv_error.errcollection2string(pio_errmsg),
                pio_errmsg
            );

            return;
        END IF;
        
--        putlogcontext(pi_fa_sctr_row.control_id,l_outcontext);
--        putlog(pi_fa_sctr_row.control_id,pi_fa_sctr_row.stag_id,
--               'eng_bill_id:'||insis_gen_v10.srv_policy_data.gengagementbillingrecord.engagement_id || '/' || insis_gen_v10.srv_policy_data.gengagementbillingrecord.num_instalments_period);
        
        --assign fixed value for SCTR

        insis_gen_v10.srv_policy_data.gengagementbillingrecord.num_instalments_period   := insis_gen_v10.gvar_pas.instalments_period_policy;
        --update 
        l_result                                                                        := insis_gen_v10.srv_policy_data.gengagementbillingrecord.updatepengagementbilling(pio_errmsg);
        IF
            NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg)
        THEN
            putlog(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                '--updatepengagementbill'
            );
            ins_error_stg(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                'ERR',
                'updatepengagementbill',
                srv_error.errcollection2string(pio_errmsg),
                pio_errmsg
            );

            return;
        END IF;
        
        
        --todo: review using pivot

        list_of_workers                                                                 := list_of_workers_cat ();
        FOR pol_ope_rate_mas_dtl_record IN (
            SELECT stg.wc_rm1 AS worker_category_rate_mas
            FROM cust_migration.fa_migr_sctr_stg stg
            WHERE stg.control_id = pi_fa_sctr_row.control_id AND stg.stag_id = pi_fa_sctr_row.stag_id --
             AND stg.wc_rm1 IS NOT NULL
            UNION
            SELECT stg.wc_rm2
            FROM cust_migration.fa_migr_sctr_stg stg
            WHERE stg.control_id = pi_fa_sctr_row.control_id AND stg.stag_id = pi_fa_sctr_row.stag_id --
             AND stg.wc_rm2 IS NOT NULL
            UNION
            SELECT stg.wc_rm3
            FROM cust_migration.fa_migr_sctr_stg stg
            WHERE stg.control_id = pi_fa_sctr_row.control_id AND stg.stag_id = pi_fa_sctr_row.stag_id --                    
             AND stg.wc_rm3 IS NOT NULL
            UNION
            SELECT stg.wc_rm4
            FROM cust_migration.fa_migr_sctr_stg stg
            WHERE stg.control_id = pi_fa_sctr_row.control_id AND stg.stag_id = pi_fa_sctr_row.stag_id --                    
             AND stg.wc_rm4 IS NOT NULL
            UNION
            SELECT stg.wc_rm5
            FROM cust_migration.fa_migr_sctr_stg stg
            WHERE stg.control_id = pi_fa_sctr_row.control_id AND stg.stag_id = pi_fa_sctr_row.stag_id --                    
             AND stg.wc_rm5 IS NOT NULL
        ) LOOP
            list_of_workers.extend;
            list_of_workers(list_of_workers.last)   := pol_ope_rate_mas_dtl_record.worker_category_rate_mas;
        END LOOP;

        list_of_workers                                                                 := set(list_of_workers);
        count_parent_obj_type                                                           := 0;
        IF
            pi_fa_sctr_row.as_is_product_code = 1
        THEN
            l_description   := 'SCTR Mineria';
        ELSIF
            pi_fa_sctr_row.as_is_product_code = 2
        THEN
            l_description   := 'SCTR No Mineria';
        END IF;        
            
        --putlog(pi_fa_sctr_row.control_id,pi_fa_sctr_row.stag_id,'l_description: '||l_description);

        << rec_worker_in_list >> FOR rec_worker IN list_of_workers.first..list_of_workers.last LOOP
            BEGIN
                SELECT (
                        CASE
                            WHEN pi_fa_sctr_row.insis_product_code = 2010 AND pi_fa_sctr_row.as_is_product_code = 1 THEN 900 + to_number(list_of_workers(rec_worker) )
                            WHEN pi_fa_sctr_row.insis_product_code = 2010 AND pi_fa_sctr_row.as_is_product_code = 2 THEN 906 + to_number(list_of_workers(rec_worker) )
                        END
                    )
                INTO
                    l_object_type
                FROM dual;

            EXCEPTION
                WHEN OTHERS THEN
                    ins_error_stg(
                        pi_fa_sctr_row.control_id,
                        pi_fa_sctr_row.stag_id,
                        'ERR',
                        'SELECT l_object_type',
                        sqlerrm,
                        pio_errmsg
                    );
--                    putlog(pi_fa_sctr_row.control_id,pi_fa_sctr_row.stag_id,'object_type.err:' || sqlerrm); 

                    return;
            END;

            putlog(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                'select ref_group_object_type: ' || l_object_type
            );
            BEGIN
                IF l_parent_obj_type IS NOT NULL
                THEN
                    l_parent_obj_type_aux   := l_parent_obj_type;
                END IF;
                SELECT gr.ref_group_object_type
                INTO
                    l_parent_obj_type
                FROM insis_cust.cfglpv_groups_allowance gr
                WHERE gr.insr_type = pi_fa_sctr_row.insis_product_code AND object_type = l_object_type;

                IF
                    l_parent_obj_type = l_parent_obj_type_aux
                THEN
                    count_parent_obj_type   := 1;
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    ins_error_stg(
                        pi_fa_sctr_row.control_id,
                        pi_fa_sctr_row.stag_id,
                        'ERR',
                        'select ref_group',
                        sqlerrm,
                        pio_errmsg
                    );

                    return;
            END;

            putlog(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                'l_parent_obj_type,count_parent_obj_type:' ||l_parent_obj_type ||',' ||count_parent_obj_type
            );

            IF l_parent_obj_type IS NOT NULL
            THEN
                IF
                    count_parent_obj_type = 0
                THEN
                    --================================================================================================
                    --PREPARE INFORMATION FOR INS_GROUP_INS
                    --================================================================================================                                   
                    putlog(
                        pi_fa_sctr_row.control_id,
                        pi_fa_sctr_row.stag_id,
                        '--INS_GROUP_INS'
                    );
                    insis_sys_v10.srv_context.setcontextattrnumber(
                        l_outcontext,
                        'OBJECT_TYPE',
                        insis_sys_v10.srv_context.integers_format,
                        nvl(l_parent_obj_type,l_object_type)
                    );

                    insis_sys_v10.srv_context.setcontextattrnumber(
                        l_outcontext,
                        'MAIN_OBJECT_ID',
                        insis_sys_v10.srv_context.integers_format,
                        NULL
                    );

                    insis_sys_v10.srv_context.setcontextattrchar(
                        l_outcontext,
                        'OGPP1',
                        nvl(l_parent_obj_type,list_of_workers(rec_worker) )
                    );

                    insis_sys_v10.srv_context.setcontextattrchar(l_outcontext,'DESCRIPTION',l_description);

                    --================================================================================================
                    -- INS_GROUP_INS
                    -- Output parameter : 
                    --================================================================================================                              
                    insis_sys_v10.srv_events.sysevent(
                        'INS_GROUP_INS',
                        l_outcontext,
                        l_outcontext,
                        pio_errmsg
                    );
                    IF
                        NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg)
                    THEN
                        putlog(
                            pi_fa_sctr_row.control_id,
                            pi_fa_sctr_row.stag_id,
                            '--INS_GROUP_INS.err'
                        );
                        ins_error_stg(
                            pi_fa_sctr_row.control_id,
                            pi_fa_sctr_row.stag_id,
                            'ERR',
                            'INS_GROUP_INS',
                            srv_error.errcollection2string(pio_errmsg),
                            pio_errmsg
                        );

                        return;
                    END IF;                                       
                        
                    --================================================================================================
                    --PREPARE INFORMATION FOR INSERT_INSURED_OBJECT EVENT
                    --================================================================================================                 

                    putlog(
                        pi_fa_sctr_row.control_id,
                        pi_fa_sctr_row.stag_id,
                        '--INSERT_INSURED_OBJECT'
                    );
                    insis_sys_v10.srv_context.setcontextattrnumber(
                        l_outcontext,
                        'POLICY_ID',
                        insis_sys_v10.srv_context.integers_format,
                        l_master_policy_id
                    );

                    insis_sys_v10.srv_context.setcontextattrnumber(
                        l_outcontext,
                        'ANNEX_ID',
                        insis_sys_v10.srv_context.integers_format,
                        insis_gen_v10.gvar_pas.def_annx_id
                    );

                    insis_sys_v10.srv_context.setcontextattrnumber(
                        l_outcontext,
                        'INSR_TYPE',
                        insis_sys_v10.srv_context.integers_format,
                        pi_fa_sctr_row.insis_product_code
                    );

                    insis_sys_v10.srv_context.setcontextattrnumber(
                        l_outcontext,
                        'OBJECT_ID',
                        insis_sys_v10.srv_context.integers_format,
                        insis_gen_v10.srv_object_data.gogroupinsrecord.object_id
                    );
                        
                    --================================================================================================
                    -- INSERT_INSURED_OBJECT
                    -- Output parameter : INSURED_OBJ_ID
                    --================================================================================================                 

                    insis_sys_v10.srv_events.sysevent(
                        'INSERT_INSURED_OBJECT',
                        l_outcontext,
                        l_outcontext,
                        pio_errmsg
                    );
                    IF
                        NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg)
                    THEN
                        putlog(
                            pi_fa_sctr_row.control_id,
                            pi_fa_sctr_row.stag_id,
                            '--INS_GROUP_INS.err'
                        );
                        ins_error_stg(
                            pi_fa_sctr_row.control_id,
                            pi_fa_sctr_row.stag_id,
                            'ERR',
                            'INS_GROUP_INS',
                            srv_error.errcollection2string(pio_errmsg),
                            pio_errmsg
                        );

                        return;
                    END IF;

                    insis_sys_v10.srv_context.getcontextattrnumber(l_outcontext,'INSURED_OBJ_ID',l_parent_ins_obj_id);
                        
                    --putlog(pi_fa_sctr_row.control_id,pi_fa_sctr_row.stag_id,'l_parent_ins_obj_id: '||l_parent_ins_obj_id);
                END IF;

                IF
                    pi_fa_sctr_row.as_is_product_code = 1
                THEN
                    UPDATE insis_gen_v10.o_group_ins
                        SET
                            ogpp1 = (
                                CASE
                                    WHEN l_object_type = 901 THEN '1'
                                    WHEN l_object_type = 902 THEN TO_CHAR(pi_fa_sctr_row.wd_rm2)
                                    WHEN l_object_type = 903 THEN TO_CHAR(pi_fa_sctr_row.wd_rm3)
                                    WHEN l_object_type = 904 THEN TO_CHAR(pi_fa_sctr_row.wd_rm4)
                                    WHEN l_object_type = 905 THEN TO_CHAR(pi_fa_sctr_row.wd_rm5)
                                END
                            )
                    WHERE object_id IN (
                            SELECT obj.object_id
                            FROM insis_gen_v10.insured_object io
                                LEFT JOIN insis_gen_v10.o_objects obj ON obj.object_id = io.object_id
                            WHERE io.group_id = l_parent_ins_obj_id AND obj.object_type = l_object_type
                        );

                END IF;

                BEGIN
                    SELECT insured_obj_id
                    INTO
                        l_ins_obj_id
                    FROM insis_gen_v10.insured_object
                    WHERE policy_id = l_master_policy_id AND object_type = l_object_type;

                EXCEPTION
                    WHEN OTHERS THEN
                        ins_error_stg(
                            pi_fa_sctr_row.control_id,
                            pi_fa_sctr_row.stag_id,
                            'ERR',
                            'SELECT INS_OBJ',
                            sqlerrm,
                            pio_errmsg
                        );

                        return;
                END;
                                
                --================================================================================================
                --PREPARE INFORMATION FOR FILL_COVERS_FOR_SELECT EVENT
                --================================================================================================            

                putlog(
                    pi_fa_sctr_row.control_id,
                    pi_fa_sctr_row.stag_id,
                    '--FILL_COVERS_FOR_SELECT'
                );
                insis_sys_v10.srv_context.setcontextattrnumber(
                    l_outcontext,
                    'INSURED_OBJ_ID',
                    insis_sys_v10.srv_context.integers_format,
                    l_ins_obj_id
                );

                insis_sys_v10.srv_context.setcontextattrnumber(
                    l_outcontext,
                    'POLICY_ID',
                    insis_sys_v10.srv_context.integers_format,
                    l_master_policy_id
                );

                insis_sys_v10.srv_context.setcontextattrnumber(
                    l_outcontext,
                    'ANNEX_ID',
                    insis_sys_v10.srv_context.integers_format,
                    insis_gen_v10.gvar_pas.def_annx_id
                );
                    
                --================================================================================================
                -- FILL_COVERS_FOR_SELECT
                -- Output parameter : TRUE or FALSE
                --================================================================================================                          

                insis_sys_v10.srv_events.sysevent(
                    'FILL_COVERS_FOR_SELECT',
                    l_outcontext,
                    l_outcontext,
                    pio_errmsg
                );
                insis_sys_v10.srv_context.getcontextattrchar(l_outcontext,'PROCEDURE_RESULT',l_procedure_result);
                IF
                    upper(l_procedure_result) = 'FALSE'
                THEN
                    putlog(
                        pi_fa_sctr_row.control_id,
                        pi_fa_sctr_row.stag_id,
                        '--FILL_COVERS_FOR_SELECT.err'
                    );
                    ins_error_stg(
                        pi_fa_sctr_row.control_id,
                        pi_fa_sctr_row.stag_id,
                        'ERR',
                        'FILL_COVERS_FOR_SELECT',
                        srv_error.errcollection2string(pio_errmsg),
                        pio_errmsg
                    );

                    return;
                END IF;
                    
                --================================================================================================
                --PREPARE INFORMATION FOR ATTACH_SELECTED_COVERS EVENT
                --================================================================================================            

                putlog(
                    pi_fa_sctr_row.control_id,
                    pi_fa_sctr_row.stag_id,
                    '--ATTACH_SELECTED_COVERS'
                );
                insis_sys_v10.srv_context.setcontextattrnumber(
                    l_outcontext,
                    'INSURED_OBJ_ID',
                    insis_sys_v10.srv_context.integers_format,
                    l_ins_obj_id
                );

                insis_sys_v10.srv_context.setcontextattrnumber(
                    l_outcontext,
                    'POLICY_ID',
                    insis_sys_v10.srv_context.integers_format,
                    l_master_policy_id
                );

                insis_sys_v10.srv_context.setcontextattrnumber(
                    l_outcontext,
                    'ANNEX_ID',
                    insis_sys_v10.srv_context.integers_format,
                    insis_gen_v10.gvar_pas.def_annx_id
                );
                    
                --================================================================================================
                -- ATTACH_SELECTED_COVERS
                -- Output parameter : TRUE or FALSE
                --================================================================================================                                    

                insis_sys_v10.srv_events.sysevent(
                    'ATTACH_SELECTED_COVERS',
                    l_outcontext,
                    l_outcontext,
                    pio_errmsg
                );
                insis_sys_v10.srv_context.getcontextattrchar(l_outcontext,'PROCEDURE_RESULT',l_procedure_result);
                IF
                    upper(l_procedure_result) = 'FALSE'
                THEN
                    putlog(
                        pi_fa_sctr_row.control_id,
                        pi_fa_sctr_row.stag_id,
                        '--ATTACH_SELECTED_COVERS.err'
                    );
                    ins_error_stg(
                        pi_fa_sctr_row.control_id,
                        pi_fa_sctr_row.stag_id,
                        'ERR',
                        'ATTACH_SELECTED_COVERS',
                        srv_error.errcollection2string(pio_errmsg),
                        pio_errmsg
                    );

                    return;
                END IF;

                BEGIN
                    SELECT rat.rate_number_rate_mas AS tariff_percent
                    INTO
                        l_tariff_percent
                    FROM (
                            SELECT stg.wc_rm1 AS worker_category_rate_mas,
                                   stg.rn_rm1 AS rate_number_rate_mas
                            FROM cust_migration.fa_migr_sctr_stg stg
                            WHERE stg.control_id = pi_fa_sctr_row.control_id AND stg.stag_id = pi_fa_sctr_row.stag_id --
                             AND stg.wc_rm1 IS NOT NULL
                            UNION
                            SELECT stg.wc_rm2,
                                   stg.rn_rm2
                            FROM cust_migration.fa_migr_sctr_stg stg
                            WHERE stg.control_id = pi_fa_sctr_row.control_id AND stg.stag_id = pi_fa_sctr_row.stag_id --    
                             AND stg.wc_rm2 IS NOT NULL
                            UNION
                            SELECT stg.wc_rm3,
                                   stg.rn_rm3
                            FROM cust_migration.fa_migr_sctr_stg stg
                            WHERE stg.control_id = pi_fa_sctr_row.control_id AND stg.stag_id = pi_fa_sctr_row.stag_id --    
                             AND stg.wc_rm3 IS NOT NULL
                            UNION
                            SELECT stg.wc_rm4,
                                   stg.rn_rm4
                            FROM cust_migration.fa_migr_sctr_stg stg
                            WHERE stg.control_id = pi_fa_sctr_row.control_id AND stg.stag_id = pi_fa_sctr_row.stag_id --    
                             AND stg.wc_rm4 IS NOT NULL
                            UNION
                            SELECT stg.wc_rm5,
                                   stg.rn_rm5
                            FROM cust_migration.fa_migr_sctr_stg stg
                            WHERE stg.control_id = pi_fa_sctr_row.control_id AND stg.stag_id = pi_fa_sctr_row.stag_id --    
                             AND stg.wc_rm5 IS NOT NULL
                        ) rat
                    WHERE rat.worker_category_rate_mas = list_of_workers(rec_worker);

                EXCEPTION
                    WHEN OTHERS THEN
                        ins_error_stg(
                            pi_fa_sctr_row.control_id,
                            pi_fa_sctr_row.stag_id,
                            'ERR',
                            'SELECT tariff_percent',
                            sqlerrm,
                            pio_errmsg
                        );

                        return;
                END;

                putlog(
                    pi_fa_sctr_row.control_id,
                    pi_fa_sctr_row.stag_id,
                    'UPD_gen_risk: l_ins_obj_id,l_master_policy_id,l_tariff_percent' ||l_ins_obj_id ||',' ||l_master_policy_id ||',' ||l_tariff_percent
                );

                --ISS069-When no tariff is assigned,it's taking value from product
                --Meanwhile a 0 value is assigned
--                IF nvl(l_tariff_percent,0) <> 0 THEN

                UPDATE insis_gen_v10.gen_risk_covered
                    SET
                        currency = pi_fa_sctr_row.currency_code,
                        tariff_percent = nvl(l_tariff_percent,0),
                        --ISS058--Percent (annual)
                        manual_prem_dimension = gvar_pas.prem_dim_p
                WHERE insured_obj_id = l_ins_obj_id AND cover_type = 'ALLOWINDDI' AND policy_id = l_master_policy_id AND annex_id = 0;

--                END IF;

            END IF;

        END LOOP rec_worker_in_list;
            
        --================================================================================================
        --PREPARE INFORMATION FOR FILL_POLICY_CONDITIONS EVENT
        --================================================================================================            

        putlog(
            pi_fa_sctr_row.control_id,
            pi_fa_sctr_row.stag_id,
            '--FILL_POLICY_CONDITIONS'
        );
        insis_sys_v10.srv_context.setcontextattrnumber(
            l_outcontext,
            'ANNEX_ID',
            insis_sys_v10.srv_context.integers_format,
            insis_gen_v10.gvar_pas.def_annx_id
        );

        insis_sys_v10.srv_context.setcontextattrnumber(
            l_outcontext,
            'POLICY_ID',
            insis_sys_v10.srv_context.integers_format,
            l_master_policy_id
        );

        insis_sys_v10.srv_context.setcontextattrnumber(
            l_outcontext,
            'INSR_TYPE',
            insis_sys_v10.srv_context.integers_format,
            pi_fa_sctr_row.insis_product_code
        );

        --================================================================================================
        -- FILL_POLICY_CONDITIONS
        -- Output parameter : TRUE or FALSE
        --================================================================================================                      

        insis_sys_v10.srv_events.sysevent(
            'FILL_POLICY_CONDITIONS',
            l_outcontext,
            l_outcontext,
            pio_errmsg
        );
        insis_sys_v10.srv_context.getcontextattrchar(l_outcontext,'PROCEDURE_RESULT',l_procedure_result);
        IF
            upper(l_procedure_result) = 'FALSE'
        THEN
            putlog(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                '--FILL_POLICY_CONDITIONS.err'
            );
            ins_error_stg(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                'ERR',
                'FILL_POLICY_CONDITIONS',
                srv_error.errcollection2string(pio_errmsg),
                pio_errmsg
            );

            return;
        END IF;

        IF
            pi_fa_sctr_row.as_is_product_code <> 0
        THEN
            UPDATE insis_gen_v10.policy_conditions
                SET
                    cond_dimension = pi_fa_sctr_row.as_is_product_code
            WHERE policy_id = l_master_policy_id AND annex_id = insis_gen_v10.gvar_pas.def_annx_id AND cond_type = 'AS_IS_SCTR';

        END IF;

        --================================================================================================
        -- CUST_POLICY_DEFAULT_PARAMS
        -- Output parameter : POLICY_ID
        --================================================================================================

        putlog(
            pi_fa_sctr_row.control_id,
            pi_fa_sctr_row.stag_id,
            '--CUST_POLICY_DEFAULT_PARAMS'
        );
        insis_sys_v10.srv_events.sysevent(
            'CUST_POLICY_DEFAULT_PARAMS',
            l_outcontext,
            l_outcontext,
            pio_errmsg
        );
        IF
            pi_fa_sctr_row.insis_product_code = 2010
        THEN
            --todo:  usar rutinas genericas de actualizar
            UPDATE insis_gen_v10.policy_conditions
                SET
                    cond_value = pi_fa_sctr_row.commiss_perc
            WHERE policy_id = l_master_policy_id AND annex_id = insis_gen_v10.gvar_pas.def_annx_id AND cond_type = 'X%_COM_ASES';

            UPDATE insis_gen_v10.policy_conditions
                SET
                    cond_dimension = pi_fa_sctr_row.activity_code
            WHERE policy_id = l_master_policy_id AND annex_id = insis_gen_v10.gvar_pas.def_annx_id AND cond_type = 'RISK_ACT_SCTR';

            UPDATE insis_gen_v10.policy_conditions
                SET
                    cond_value = pi_fa_sctr_row.min_prem_issue
            WHERE policy_id = l_master_policy_id AND annex_id = insis_gen_v10.gvar_pas.def_annx_id AND cond_type = 'PRIM_MIN_EMISION';

            UPDATE insis_gen_v10.policy_conditions
                SET
                    cond_value = pi_fa_sctr_row.min_prem_attach
            WHERE policy_id = l_master_policy_id AND annex_id = insis_gen_v10.gvar_pas.def_annx_id AND cond_type = 'PRIM_MIN_INCLUSION';

            UPDATE insis_gen_v10.policy_conditions
                SET
                    cond_value = pi_fa_sctr_row.iss_exp_percentage
            WHERE policy_id = l_master_policy_id AND annex_id = insis_gen_v10.gvar_pas.def_annx_id AND cond_type = 'ISSU_EXPENSE_SCTR';

            UPDATE insis_gen_v10.policy_conditions
                SET
                    cond_value = pi_fa_sctr_row.min_iss_expenses
            WHERE policy_id = l_master_policy_id AND annex_id = insis_gen_v10.gvar_pas.def_annx_id AND cond_type = 'MIN_ISSUE_EXPENSE';

            UPDATE insis_gen_v10.policy_conditions
                SET
                    cond_dimension = pi_fa_sctr_row.calculation_type
            WHERE policy_id = l_master_policy_id AND annex_id = insis_gen_v10.gvar_pas.def_annx_id AND cond_type = 'TYPE_CALC';

            UPDATE insis_gen_v10.policy_conditions
                SET
                    cond_dimension = pi_fa_sctr_row.billing_type
            WHERE policy_id = l_master_policy_id AND annex_id = insis_gen_v10.gvar_pas.def_annx_id AND cond_type = 'TIPO_FACTURATION';

            UPDATE insis_gen_v10.policy_conditions
                SET
                    cond_dimension = pi_fa_sctr_row.billing_way
            WHERE policy_id = l_master_policy_id AND annex_id = insis_gen_v10.gvar_pas.def_annx_id AND cond_type = 'FACTURA_POR';

--            IF pi_fa_sctr_row.warranty_clause_flag = 'Y' THEN

            UPDATE insis_gen_v10.policy_conditions
                SET
--                    cond_value = pi_fa_sctr_row.warranty_clause_flag
                    cond_dimension = (
                        CASE
                            WHEN pi_fa_sctr_row.tender_flag = 'Y' THEN 2
                            ELSE
                                1
                        END
                    )
            WHERE policy_id = l_master_policy_id AND annex_id = insis_gen_v10.gvar_pas.def_annx_id AND cond_type = 'LICITACION_TENDER';

--            END IF;

            UPDATE insis_gen_v10.policy_conditions
                SET
                    cond_dimension = (
                        CASE
                            WHEN pi_fa_sctr_row.gratuity_flag = 'N' THEN 1
                            WHEN pi_fa_sctr_row.gratuity_flag = 'Y' THEN 2
                        END
                    )
            WHERE policy_id = l_master_policy_id AND annex_id = insis_gen_v10.gvar_pas.def_annx_id AND cond_type = 'GRATUITY_INDICATOR';

            UPDATE insis_gen_v10.policy_conditions
                SET
                    cond_dimension = (
                        CASE
                            WHEN pi_fa_sctr_row.consortium_flag = 'N' THEN 1
                            WHEN pi_fa_sctr_row.consortium_flag = 'Y' THEN 2
                        END
                    )
            WHERE policy_id = l_master_policy_id AND annex_id = insis_gen_v10.gvar_pas.def_annx_id AND cond_type = 'CONSORCIO';

            UPDATE insis_gen_v10.policy_conditions
                SET
                    cond_dimension = (
                        CASE
                            WHEN pi_fa_sctr_row.tender_flag = 'N' THEN 1
                            WHEN pi_fa_sctr_row.tender_flag = 'Y' THEN 2
                        END
                    )
            WHERE policy_id = l_master_policy_id AND annex_id = insis_gen_v10.gvar_pas.def_annx_id AND cond_type = 'LICITACION_TENDER';

            UPDATE insis_gen_v10.policy_conditions
                SET
                    cond_value = pi_fa_sctr_row.policy_salud
            WHERE policy_id = l_master_policy_id AND annex_id = insis_gen_v10.gvar_pas.def_annx_id AND cond_type = 'POL_SCTR_HEALTH';

        END IF;   
         
        --================================================================================================
        --UPDATE POLICY_NAMES  CONCAT PRODUCT AND AS_IS
        --================================================================================================ 

        putlog(
            pi_fa_sctr_row.control_id,
            pi_fa_sctr_row.stag_id,
            '--POLICY_NAMES  CONCAT PRODUCT AND AS_IS'
        );
        IF
            NOT insis_cust.cust_policy.concatproductnamewithas_is(l_master_policy_id,0,pio_errmsg)
        THEN
            putlog(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                '--ConcatProductNameWithAS_IS.err'
            );
            ins_error_stg(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                'ERR',
                'ConcatProductNameWithAS_IS',
                srv_error.errcollection2string(pio_errmsg),
                pio_errmsg
            );

            return;
        END IF;

        --================================================================================================
        --PREPARE INFORMATION FOR LOAD_QUEST EVENT        
        --================================================================================================            

        putlog(
            pi_fa_sctr_row.control_id,
            pi_fa_sctr_row.stag_id,
            '--LOAD_QUEST'
        );
        insis_sys_v10.srv_context.setcontextattrchar(l_outcontext,'REFERENCE_TYPE','POLICY');
        insis_sys_v10.srv_context.setcontextattrchar(l_outcontext,'TO_LOAD','Y');
        insis_sys_v10.srv_context.setcontextattrnumber(
            l_outcontext,
            'POLICY_ID',
            insis_sys_v10.srv_context.integers_format,
            l_master_policy_id
        );

        insis_sys_v10.srv_context.setcontextattrnumber(
            l_outcontext,
            'ANNEX_ID',
            insis_sys_v10.srv_context.integers_format,
            insis_gen_v10.gvar_pas.def_annx_id
        );

        insis_sys_v10.srv_context.setcontextattrnumber(
            l_outcontext,
            'PHOLDER_ID',
            insis_sys_v10.srv_context.integers_format,
            insis_gen_v10.srv_policy_data.gpolicyrecord.client_id
        );
            
        --================================================================================================
        -- LOAD_QUEST
        -- Output parameter : TRUE or FALSE
        --================================================================================================          

        insis_sys_v10.srv_events.sysevent(
            'LOAD_QUEST',
            l_outcontext,
            l_outcontext,
            pio_errmsg
        );
        insis_sys_v10.srv_context.getcontextattrchar(l_outcontext,'PROCEDURE_RESULT',l_procedure_result);
        IF
            upper(l_procedure_result) = 'FALSE'
        THEN
            putlog(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                '--LOAD_QUEST.err'
            );
            ins_error_stg(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                'ERR',
                'LOAD_QUEST',
                srv_error.errcollection2string(pio_errmsg),
                pio_errmsg
            );

            return;
        END IF;

        --================================================================================================
        --PREPARE INFORMATION FOR GET_POL_QUEST EVENT
        --================================================================================================            

        putlog(
            pi_fa_sctr_row.control_id,
            pi_fa_sctr_row.stag_id,
            '--GET_POL_QUEST'
        );
        insis_sys_v10.srv_context.setcontextattrnumber(
            l_outcontext,
            'POLICY_ID',
            insis_sys_v10.srv_context.integers_format,
            l_master_policy_id
        );

        insis_sys_v10.srv_context.setcontextattrnumber(
            l_outcontext,
            'ANNEX_ID',
            insis_sys_v10.srv_context.integers_format,
            insis_gen_v10.gvar_pas.def_annx_id
        );

        insis_sys_v10.srv_context.setcontextattrchar(l_outcontext,'QUEST_CODE','EPOLR');       
            
        --================================================================================================
        -- GET_POL_QUEST
        -- Output parameter : srv_question_data.gQuestionRecord/srv_question_data.gQuestionTable
        --================================================================================================                  
        insis_sys_v10.srv_events.sysevent(
            'GET_POL_QUEST',
            l_outcontext,
            l_outcontext,
            pio_errmsg
        );
        IF
            NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg)
        THEN
            putlog(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                '--GET_POL_QUEST.err'
            );
            ins_error_stg(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                'ERR',
                'GET_POL_QUEST',
                srv_error.errcollection2string(pio_errmsg),
                pio_errmsg
            );

            return;
        END IF;        

        --================================================================================================
        --PREPARE INFORMATION FOR UPD_QUEST EVENT
        --================================================================================================            

        putlog(
            pi_fa_sctr_row.control_id,
            pi_fa_sctr_row.stag_id,
            '--UPD_QUEST'
        );
        insis_sys_v10.srv_context.setcontextattrnumber(
            l_outcontext,
            'ID',
            insis_sys_v10.srv_context.integers_format,
            insis_sys_v10.srv_question_data.gquestionrecord.id
        );

        insis_sys_v10.srv_context.setcontextattrchar(
            l_outcontext,
            'QUEST_ANSWER',
            (CASE
                WHEN pi_fa_sctr_row.elec_pol_flag = 'Y' THEN 3
                WHEN pi_fa_sctr_row.elec_pol_flag = 'N' THEN 4
            END)
        );
            
        --================================================================================================
        -- UPD_QUEST
        -- Output parameter : srv_question_data.gQuestionRecord/srv_question_data.gQuestionTable
        --================================================================================================

        insis_sys_v10.srv_events.sysevent(
            'UPD_QUEST',
            l_outcontext,
            l_outcontext,
            pio_errmsg
        );
        IF
            NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg)
        THEN
            putlog(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                '--UPD_QUEST.err'
            );
            ins_error_stg(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                'ERR',
                'UPD_QUEST',
                srv_error.errcollection2string(pio_errmsg),
                pio_errmsg
            );

            return;
        END IF;
        
        --ISS028-Activity detail could be null        

        IF pi_fa_sctr_row.activity_detail IS NOT NULL
        THEN
            --================================================================================================
            --PREPARE INFORMATION FOR GET_POL_QUEST EVENT
            --================================================================================================            
            putlog(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                '--GET_POL_QUEST_2010.03'
            );
            insis_sys_v10.srv_context.setcontextattrnumber(
                l_outcontext,
                'POLICY_ID',
                insis_sys_v10.srv_context.integers_format,
                l_master_policy_id
            );

            insis_sys_v10.srv_context.setcontextattrnumber(
                l_outcontext,
                'ANNEX_ID',
                insis_sys_v10.srv_context.integers_format,
                insis_gen_v10.gvar_pas.def_annx_id
            );

            insis_sys_v10.srv_context.setcontextattrchar(l_outcontext,'QUEST_CODE','2010.03');       
                
            --================================================================================================
            -- GET_POL_QUEST
            -- Output parameter : srv_question_data.gQuestionRecord/srv_question_data.gQuestionTable
            --================================================================================================      
            insis_sys_v10.srv_events.sysevent(
                'GET_POL_QUEST',
                l_outcontext,
                l_outcontext,
                pio_errmsg
            );
            IF
                NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg)
            THEN
                putlog(
                    pi_fa_sctr_row.control_id,
                    pi_fa_sctr_row.stag_id,
                    '--GET_POL_QUEST.err'
                );
                ins_error_stg(
                    pi_fa_sctr_row.control_id,
                    pi_fa_sctr_row.stag_id,
                    'ERR',
                    'GET_POL_QUEST',
                    srv_error.errcollection2string(pio_errmsg),
                    pio_errmsg
                );

                return;
            END IF;        
    
            --================================================================================================
            --PREPARE INFORMATION FOR UPD_QUEST EVENT
            --================================================================================================            

            putlog(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                '--UPD_QUEST_2010.03'
            );
            insis_sys_v10.srv_context.setcontextattrnumber(
                l_outcontext,
                'ID',
                insis_sys_v10.srv_context.integers_format,
                insis_sys_v10.srv_question_data.gquestionrecord.id
            );

            insis_sys_v10.srv_context.setcontextattrchar(
                l_outcontext,
                'QUEST_ANSWER',
                pi_fa_sctr_row.activity_detail
            );
        
            --================================================================================================
            -- UPD_QUEST
            -- Output parameter : srv_question_data.gQuestionRecord/srv_question_data.gQuestionTable
            --================================================================================================             
            insis_sys_v10.srv_events.sysevent(
                'UPD_QUEST',
                l_outcontext,
                l_outcontext,
                pio_errmsg
            );
            IF
                NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg)
            THEN
                putlog(
                    pi_fa_sctr_row.control_id,
                    pi_fa_sctr_row.stag_id,
                    'UPD_QUEST_2010.03.err'
                );
                ins_error_stg(
                    pi_fa_sctr_row.control_id,
                    pi_fa_sctr_row.stag_id,
                    'ERR',
                    'UPD_QUEST_2010.03',
                    srv_error.errcollection2string(pio_errmsg),
                    pio_errmsg
                );

                return;
            END IF;

        END IF;
                            
        --================================================================================================
        --PREPARE INFORMATION FOR INSERT_ENDORSEMENT EVENT
        --================================================================================================            

        putlog(
            pi_fa_sctr_row.control_id,
            pi_fa_sctr_row.stag_id,
            '--INSERT_ENDORSEMENT'
        );
        insis_sys_v10.srv_context.setcontextattrnumber(
            l_outcontext,
            'POLICY_ID',
            insis_sys_v10.srv_context.integers_format,
            l_master_policy_id
        );

        insis_sys_v10.srv_context.setcontextattrnumber(
            l_outcontext,
            'ANNEX_ID',
            insis_sys_v10.srv_context.integers_format,
            insis_gen_v10.gvar_pas.def_annx_id
        );
            
        --================================================================================================
        -- INSERT_ENDORSEMENT
        -- Output parameter : 
        --================================================================================================           

        insis_sys_v10.srv_events.sysevent(
            'INSERT_ENDORSEMENT',
            l_outcontext,
            l_outcontext,
            pio_errmsg
        );
        insis_sys_v10.srv_context.getcontextattrchar(l_outcontext,'PROCEDURE_RESULT',l_procedure_result);
        IF
            upper(l_procedure_result) = 'FALSE'
        THEN
            putlog(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                'INSERT_ENDORSEMENT'
            );
            ins_error_stg(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                'ERR',
                'INSERT_ENDORSEMENT',
                srv_error.errcollection2string(pio_errmsg),
                pio_errmsg
            );

            return;
        END IF;
        
        --ISS005 - Removes specific endorsements for a minning policy

        DELETE insis_gen_v10.policy_endorsements WHERE policy_id = l_master_policy_id AND endorsement_code NOT IN (
                614
            );

        IF
            pi_fa_sctr_row.spec_pen_clause_flag = 'Y'
        THEN
            UPDATE insis_gen_v10.policy_endorsements
                SET
                    text = pi_fa_sctr_row.spec_pen_clause_detail
            WHERE policy_id = l_master_policy_id AND endorsement_code = 614 AND cover_type IS NULL;

        END IF;

        --================================================================================================
        --PREPARE INFORMATION FOR CALC_PREM EVENT
        --================================================================================================            

        putlog(
            pi_fa_sctr_row.control_id,
            pi_fa_sctr_row.stag_id,
            '--CALC_PREM'
        );
        insis_sys_v10.srv_context.setcontextattrnumber(
            l_outcontext,
            'POLICY_ID',
            insis_sys_v10.srv_context.integers_format,
            l_master_policy_id
        );

        insis_sys_v10.srv_context.setcontextattrnumber(
            l_outcontext,
            'ANNEX_ID',
            insis_sys_v10.srv_context.integers_format,
            insis_gen_v10.gvar_pas.def_annx_id
        );       
            
        --================================================================================================
        -- CALC_PREM
        -- Output parameter : 
        --================================================================================================           

        insis_sys_v10.srv_events.sysevent(
            'CALC_PREM',
            l_outcontext,
            l_outcontext,
            pio_errmsg
        );
        IF
            NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg)
        THEN
            putlog(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                'CALC_PREM'
            );
            ins_error_stg(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                'ERR',
                'CALC_PREM',
                srv_error.errcollection2string(pio_errmsg),
                pio_errmsg
            );

            return;
        END IF;

        --================================================================================================
        -- APPL_CONF
        -- Output parameter : 
        --================================================================================================               

        putlog(
            pi_fa_sctr_row.control_id,
            pi_fa_sctr_row.stag_id,
            '--APPL_CONF'
        );
        insis_sys_v10.srv_events.sysevent(
            'APPL_CONF',
            l_outcontext,
            l_outcontext,
            pio_errmsg
        );
        IF
            NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg)
        THEN
            putlog(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                'APPL_CONF'
            );
            ins_error_stg(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                'ERR',
                'APPL_CONF',
                srv_error.errcollection2string(pio_errmsg),
                pio_errmsg
            );

            return;
        END IF;                        

        --ISS029-Defines final policy status           

        IF
            pi_fa_sctr_row.policy_state = gvar_pas.psm_open
        THEN
    
            --================================================================================================
            -- APPL_CONV
            -- Output parameter : 
            --================================================================================================              
            putlog(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                '--APPL_CONV'
            );
            insis_sys_v10.srv_events.sysevent(
                'APPL_CONV',
                l_outcontext,
                l_outcontext,
                pio_errmsg
            );
            IF
                NOT insis_sys_v10.srv_error.rqstatus(pio_errmsg)
            THEN
                putlog(
                    pi_fa_sctr_row.control_id,
                    pi_fa_sctr_row.stag_id,
                    'APPL_CONV'
                );
                ins_error_stg(
                    pi_fa_sctr_row.control_id,
                    pi_fa_sctr_row.stag_id,
                    'ERR',
                    'APPL_CONV',
                    srv_error.errcollection2string(pio_errmsg),
                    pio_errmsg
                );

                return;
            END IF;

        END IF;

        putlog(
            pi_fa_sctr_row.control_id,
            pi_fa_sctr_row.stag_id,
            'Update policy policy_name policy_no date_covered'
        );
        UPDATE insis_gen_v10.policy
            SET
                policy_no = pi_fa_sctr_row.policy_name,
                policy_name = pi_fa_sctr_row.policy_name,
                date_covered = l_date_covered
        WHERE policy_id = l_master_policy_id;

        UPDATE cust_migration.fa_migr_sctr_stg
            SET
                att_policy_id = l_master_policy_id
        WHERE control_id = pi_fa_sctr_row.control_id AND stag_id = pi_fa_sctr_row.stag_id;

        putlog(
            pi_fa_sctr_row.control_id,
            pi_fa_sctr_row.stag_id,
            'sctr_record_proc|end'
        );
    EXCEPTION
        WHEN OTHERS THEN
            putlog(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                'sctr_record_proc|end_err' || sqlerrm
            );
            ins_error_stg(
                pi_fa_sctr_row.control_id,
                pi_fa_sctr_row.stag_id,
                'ERR',
                'sctr_record_proc',
                sqlerrm,
                pio_errmsg
            );

    END sctr_record_proc;

    PROCEDURE get_last_record_for_report (
        po_poller_id OUT NUMBER,
        po_file_name OUT VARCHAR2,
        po_success_flag OUT INTEGER
    )
        IS
    BEGIN
        l_log_proc        := '0';
        putlog(
            0,
            0,
            'get_last_record_for_report|start| ' || po_poller_id
        );
        po_success_flag   := 1;
        SELECT
--            poller_name,
         sys_poller_process_ctrl_id,
               substr(
                file_name,
                0,
                instr(file_name,'.') - 1
            ) ||'_' ||sys_poller_process_ctrl_id ||'_' ||TO_CHAR(date_init,'YYYYMMDD') ||'_' ||TO_CHAR(date_init,'HH24MISS') ||'.xlsx'
--            date_init,
--            date_end,
--            status
        INTO
--            po_poller_name,
            po_poller_id,po_file_name
--            po_date_ini,
--            po_date_end,
--            po_status
        FROM insis_cust_lpv.sys_poller_process_ctrl
        WHERE sys_poller_process_ctrl_id = (
                SELECT control_id
                FROM (   --recover oldest process pending process that has data processed (status 2 or 3))
                        SELECT control_id
                        FROM cust_migration.fa_migr_sctr_err ctrl
                        WHERE stag_id = 0 AND errseq = 0 AND errtype = 'REP' --record ready for report
                         AND EXISTS (
                                SELECT 1
                                FROM cust_migration.fa_migr_sctr_stg stg
                                WHERE stg.control_id = ctrl.control_id AND stg.att_status IN (
                                        lc_stat_rec_valid,lc_stat_rec_error
                                    )
                            )
                        ORDER BY control_id ASC
                    )
                WHERE ROWNUM = 1
            );

        putlog(
            po_poller_id,
            0,
            'get_last_record_for_report|end| ' || po_poller_id
        );
    EXCEPTION
        WHEN OTHERS THEN
            po_success_flag   := 0;
            putlog(
                0,
                0,
                'get_last_record_for_report|end_err| ' || sqlerrm
            );
    END get_last_record_for_report;

    PROCEDURE upd_last_record_for_report (
        pi_control_id_rep IN NUMBER,
        pi_file_id IN NUMBER,
        pi_control_id_proc IN NUMBER
    )
        IS
    BEGIN
        l_log_proc   := pi_control_id_rep;
        putlog(
            pi_control_id_rep,
            0,
            'update_sctr_master_process_status|start|file_id: ' || pi_control_id_rep
        );
        UPDATE insis_cust_lpv.sys_poller_process_ctrl
            SET
                file_id = upd_last_record_for_report.pi_file_id
        WHERE sys_poller_process_ctrl_id = pi_control_id_rep;

        insis_cust_lpv.sys_schema_utils.update_poller_process_status(pi_control_id_rep,'SUCCESS');
        UPDATE cust_migration.fa_migr_sctr_err
            SET
                errtype = 'GEN',
                errmess = '--Report generated--'
        WHERE control_id = pi_control_id_proc AND stag_id = 0 AND errseq = 0 AND errtype = 'REP';

        COMMIT;
        putlog(pi_control_id_rep,0,'upd_last_record_report|end');
    EXCEPTION
        WHEN OTHERS THEN
            putlog(
                pi_control_id_rep,
                0,
                'upd_last_record_report|end_err|' || sqlerrm
            );
    END upd_last_record_for_report;

    PROCEDURE ins_error_stg (
        pi_control_id IN fa_migr_sctr_err.control_id%TYPE,
        pi_stag_id IN fa_migr_sctr_err.stag_id%TYPE,
--        pi_errseq       IN      fa_migr_sctr_err.errseq%type,
        pi_errtype IN fa_migr_sctr_err.errtype%TYPE,
        pi_errcode IN fa_migr_sctr_err.errcode%TYPE,
        pi_errmess IN fa_migr_sctr_err.errmess%TYPE,
        pio_errmsg IN OUT srverr
    ) IS
        PRAGMA autonomous_transaction;
        l_errmsg srverrmsg;
    BEGIN
        l_errseq   := l_errseq + 1;
--        insis_sys_v10.srv_error.seterrormsg(l_errmsg,pi_fn_name,pi_error_id);
--        insis_sys_v10.srv_error.seterrormsg(l_errmsg,pio_errmsg);
        INSERT INTO fa_migr_sctr_err (
            control_id,
            stag_id,
            errseq,
            errtype,
            errcode,
            errmess
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
            putlog(
                pi_control_id,
                0,
                'ins_error_stg.err|' ||pi_errcode ||'|' ||sqlerrm
            );
            srv_error.setsyserrormsg(l_errmsg,'insert_error_stg',sqlerrm);
            srv_error.seterrormsg(l_errmsg,pio_errmsg);
    END ins_error_stg;

END fa_cust_migr_sctr;