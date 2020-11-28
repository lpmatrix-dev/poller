--control pollers
SELECT c.*,--to_char(sysdate - c.date_init,'dd/mm/yyyy HH24:MI:SS') elap,
--     (select count(1) from cust_migration.lpv_sctr_master_stg s where s.ctrl_id = c.sys_poller_process_ctrl_id ) cant,
       (
            SELECT COUNT(1)
            FROM cust_migration.fa_migr_sctr_stg s
            WHERE s.control_id   = c.sys_poller_process_ctrl_id
      ) cant
FROM insis_cust_lpv.sys_poller_process_ctrl c
WHERE 1 = 1
      AND poller_name LIKE 'XLS_MIGR_SCTR%' 
--    and sys_poller_process_ctrl_id = 42249
ORDER BY 1 DESC;

--control pollers sctr
SELECT *
FROM cust_migration.lpv_sctr_master_ctrl
--WHERE
--    poller_name = 'XLS_UPLOAD_SCTR'
--    and sys_poller_process_ctrl_id = sys_ctrl_id
ORDER BY 1 DESC;


--log v2
--delete (
SELECT *
FROM sta_log
WHERE 1 = 1 AND table_name = 'FA_CUST_MIGR_SCTR' --'LPV_SCTR_MASTER'
 AND batch_id LIKE (
        SELECT MAX(sys_poller_process_ctrl_id) || '%'
        FROM insis_cust_lpv.sys_poller_process_ctrl c
        WHERE 1 = 1 AND poller_name = 'XLS_MIGR_SCTR'
    )
--    and log_message like '%[8]%'
ORDER BY batch_id,
--    rec_count,
--    log_message,
 time_stamp 
--)
;

--data
SELECT *
FROM cust_migration.fa_migr_sctr_stg
WHERE 1 = 1 AND control_id IN --(45379)
 (
        SELECT 56714--max(SYS_POLLER_PROCESS_CTRL_ID)
        FROM insis_cust_lpv.sys_poller_process_ctrl c
        WHERE 1 = 1 AND poller_name = 'XLS_MIGR_SCTR'
    )
ORDER BY control_id DESC,stag_id;

--select *
--from cust_migration.lpv_sctr_master_stg
--where 1=1
--and ctrl_id in (44290)
----and policy_id = 100000071387
--order by ctrl_id desc, stg_id
--;


--error
SELECT *
FROM cust_migration.fa_migr_sctr_err
WHERE 1 = 1 AND control_id IN (
        SELECT MAX(sys_poller_process_ctrl_id)
        FROM insis_cust_lpv.sys_poller_process_ctrl c
        WHERE 1 = 1 AND poller_name LIKE 'XLS_MIGR_SCTR%'
    )
--and policy_id = 100000071387
--and errseq = 0
ORDER BY control_id DESC,stag_id;

--select *
--from cust_migration.lpv_sctr_master_error_log
--where 1=1
--and sys_poller_process_ctrl_id = 44290
--order by sys_poller_process_ctrl_id desc, stg_id 
--;

--------------------------
--reporte
SELECT to_number(mas.fila) AS fila,
       mas.att_policy_id AS policy_id,
       pol.policy_name AS policy_name,
       (
            SELECT peo.name
            FROM insis_people_v10.p_people peo
                  INNER JOIN insis_cust.intrf_lpv_people_ids ids ON peo.man_id   = ids.man_id
            WHERE ids.legacy_id   = mas.policy_holder_code
      ) AS policy_holder,
       TO_CHAR(pol.insr_begin,'DD-MM-YYYY') AS begin_date,
       TO_CHAR(pol.insr_end,'DD-MM-YYYY') AS end_date,
       pol.policy_state ||'-' ||hps.name AS policy_state,
       'SUCCESS' AS result,
       NULL AS err_detail
FROM cust_migration.lpv_sctr_master_stg mas
      LEFT JOIN insis_gen_v10.policy pol ON pol.policy_id   = mas.att_policy_id
      LEFT JOIN insis_gen_v10.hst_policy_state hps ON pol.policy_state   = hps.id
WHERE mas.ctrl_id      =:current_batch_id
      AND mas.att_status   = '2'
UNION
SELECT to_number(mas.fila) AS fila,
       NULL AS policy_id,
       NULL AS policy_name,
       NULL AS policy_holder,
       NULL AS begin_date,
       NULL AS end_date,
       NULL AS policy_state,
       'ERROR' AS result,
       err.error_message AS err_detail
FROM cust_migration.lpv_sctr_master_stg mas
      INNER JOIN cust_migration.lpv_sctr_master_error_log err ON mas.ctrl_id   = err.sys_poller_process_ctrl_id
      AND mas.fila      = err.fila
WHERE mas.ctrl_id      =:current_batch_id
      AND mas.att_status   = '3'
ORDER BY 1;


SELECT
    to_number(mas.fila)                                                                                                                                                                          AS fila,
    mas.att_policy_id                                                                                                                                                                            AS policy_id,
    pol.policy_name                                                                                                                                                                              AS policy_name,
    (
        SELECT
            peo.NAME
        FROM
                 insis_people_v10.p_people peo
            INNER JOIN insis_cust.intrf_lpv_people_ids ids ON peo.man_id = ids.man_id
        WHERE
            ids.legacy_id = mas.policy_holder_code
    )                      AS policy_holder,
    pol.insr_begin                                                                                                                                                                               AS begin_date,
    pol.insr_end                                                                                                                                                                                 AS end_date,
    pol.policy_state || '-' || hps.NAME                                                                                                                                                              AS policy_state,
    'SUCCESS'                                                                                                                                                                                    AS result,
    NULL                                                                                                                                                                                         AS err_detail
FROM
    cust_migration.lpv_sctr_master_stg    mas
    LEFT JOIN insis_gen_v10.POLICY                  pol ON pol.policy_id = mas.att_policy_id
    LEFT JOIN insis_gen_v10.hst_policy_state        hps ON pol.policy_state = hps.ID
WHERE
        mas.ctrl_id = 43610
    AND mas.att_status = '2'
UNION
SELECT
    to_number(mas.fila)       AS fila,
    to_number('')             AS policy_id,
    ''                        AS policy_name,
    ''                        AS policy_holder,
    to_date('')                        AS begin_date,
    to_date('')                        AS end_date,
    ''                        AS policy_state,
    'ERROR'                   AS result,
    err.error_message         AS err_detail
FROM
         cust_migration.lpv_sctr_master_stg mas
    INNER JOIN cust_migration.lpv_sctr_master_error_log err ON mas.ctrl_id = err.sys_poller_process_ctrl_id
                                                               AND mas.fila = err.fila
WHERE
        mas.ctrl_id = 43610
    AND mas.att_status = '3'
ORDER BY
    1
;

--------------------------------------
SELECT 
                    TO_NUMBER(mas.fila) AS FILA,
                    mas.att_policy_id AS POLICY_ID,
                    pol.policy_name AS POLICY_NAME,
                    (SELECT peo.NAME 
                    FROM insis_people_v10.p_people peo
                    INNER JOIN insis_cust.intrf_lpv_people_ids ids ON peo.man_id=ids.man_id
                    WHERE ids.insunix_code=mas.policy_holder_code) AS POLICY_HOLDER,
                    pol.insr_begin AS BEGIN_DATE,
                    pol.insr_end AS END_DATE,
                    pol.policy_state||'-'||hps.NAME AS POLICY_STATE,
                    'SUCCESS' AS RESULT,
                    NULL AS ERR_DETAIL
				FROM cust_migration.lpv_sctr_master_stg mas
				LEFT JOIN insis_gen_v10.POLICY pol ON pol.policy_id=mas.att_policy_id
				LEFT JOIN insis_gen_v10.hst_policy_state hps ON pol.policy_state=hps.ID
				WHERE mas.ctrl_id=:current_batch_id
				AND mas.att_status='2'
				UNION
				SELECT 
                    TO_NUMBER(mas.fila) AS FILA,
                    TO_NUMBER('') AS POLICY_ID,
                    '' AS POLICY_NAME,
                    '' AS POLICY_HOLDER,
                    NULL AS BEGIN_DATE,
                    NULL AS END_DATE,
                    '' AS POLICY_STATE,
                    'ERROR' AS RESULT,
                    err.error_message AS ERR_DETAIL
				FROM cust_migration.lpv_sctr_master_stg mas 
				INNER JOIN cust_migration.lpv_sctr_master_error_log err ON mas.ctrl_id=err.sys_poller_process_ctrl_id AND mas.fila=err.fila
				WHERE mas.ctrl_id=:current_batch_id
				AND mas.att_status='3'
				ORDER BY 1
;
-----------

--limpiar  reporte con errores
--campo esta de tipo varchar...pasar a numero
    UPDATE (
        SELECT *
        FROM cust_migration.lpv_sctr_master_stg mas
        WHERE 1=1
--        and mas.sys_poller_process_ctrl_id = 42057
--        AND mas.att_status = '3'
        AND REGEXP_LIKE(mas.fila, '[^0-9]')
    ) set fila = NULL



------------------------
--list of workers
SELECT stg.wc_rm1 AS worker_category_rate_mas
FROM cust_migration.lpv_sctr_master_stg stg
WHERE stg.sys_poller_process_ctrl_id = 41872 AND stg.wc_rm1 IS NOT NULL
UNION
SELECT stg.wc_rm2
FROM cust_migration.lpv_sctr_master_stg stg
WHERE stg.sys_poller_process_ctrl_id = 41872 AND stg.wc_rm2 IS NOT NULL
UNION
SELECT stg.wc_rm3
FROM cust_migration.lpv_sctr_master_stg stg
WHERE stg.sys_poller_process_ctrl_id = 41872 AND stg.wc_rm3 IS NOT NULL
UNION
SELECT stg.wc_rm4
FROM cust_migration.lpv_sctr_master_stg stg
WHERE stg.sys_poller_process_ctrl_id = 41872 AND stg.wc_rm4 IS NOT NULL
UNION
SELECT stg.wc_rm5
FROM cust_migration.lpv_sctr_master_stg stg
WHERE stg.sys_poller_process_ctrl_id = 41872 AND stg.wc_rm5 IS NOT NULL;
--result: 1,2,3,4,5


--tasas
SELECT rat.rate_number_rate_mas AS tariff_percent
FROM (
        SELECT stg.wc_rm1 AS worker_category_rate_mas,
               stg.rn_rm1 AS rate_number_rate_mas
        FROM cust_migration.lpv_sctr_master_stg stg
        WHERE stg.sys_poller_process_ctrl_id = 41872 AND stg.wc_rm1 IS NOT NULL
        UNION
        SELECT stg.wc_rm2,
               stg.rn_rm2
        FROM cust_migration.lpv_sctr_master_stg stg
        WHERE stg.sys_poller_process_ctrl_id = 41872 AND stg.wc_rm2 IS NOT NULL
        UNION
        SELECT stg.wc_rm3,
               stg.rn_rm3
        FROM cust_migration.lpv_sctr_master_stg stg
        WHERE stg.sys_poller_process_ctrl_id = 41872 AND stg.wc_rm3 IS NOT NULL
        UNION
        SELECT stg.wc_rm4,
               stg.rn_rm4
        FROM cust_migration.lpv_sctr_master_stg stg
        WHERE stg.sys_poller_process_ctrl_id = 41872 AND stg.wc_rm4 IS NOT NULL
        UNION
        SELECT stg.wc_rm5,
               stg.rn_rm5
        FROM cust_migration.lpv_sctr_master_stg stg
        WHERE stg.sys_poller_process_ctrl_id = 41872 AND stg.wc_rm5 IS NOT NULL
    ) rat
WHERE rat.worker_category_rate_mas = 1;

SELECT *
FROM insis_gen_cfg_v10.srv_messages
WHERE 1 = 1
--and msg_id like 'Upload_SctrMaster%'
 AND lower(msg_text) LIKE '%broker%';


SRV_MESSAGES_LANG


    
--===============================================================================
--Queries validacion tras carga
SELECT *
FROM insis_sys_v10.quest_questions
WHERE 1 = 1 AND policy_id IN (
        SELECT stg.att_policy_id
        FROM cust_migration.lpv_sctr_master_stg stg
        WHERE ctrl_id = 42249
    ) AND quest_id = 'EPOLR' AND quest_answer NOT IN (
        '1','2'
    )
ORDER BY policy_id,id;

 

SELECT * 
FROM insis_gen_v10.policy_commissions
WHERE policy_id = 100000080282
;

SELECT * 
FROM insis_gen_v10.POLICY
WHERE policy_id = 100000084084
;

--version antigua, con codigo vtime
SELECT
    dat.stg_id, dat.broker_code, i.insunix_code, i.legacy_id
FROM cust_migration.lpv_sctr_master_stg dat
    LEFT JOIN insis_cust.intrf_lpv_people_ids i ON (i.legacy_id = dat.broker_code)
WHERE
    dat.ctrl_id = 43773
    ORDER BY dat.stg_id
;

--grupos
SELECT o_gr.* 
FROM insis_gen_v10.o_group_ins o_gr
-- LEFT JOIN insis_gen_v10.o_objects obj ON (obj.object_id = o_gr.object_id)
INNER JOIN insis_gen_v10.insured_object io ON (io.object_id = o_gr.object_id)
INNER JOIN cust_migration.fa_migr_sctr_stg fa_stg ON (fa_stg.att_policy_id = io.policy_id)
WHERE 1=1
AND fa_stg.control_id = 53229
            

;

SELECT
      *
FROM
    insis_people_v10.p_clients
WHERE
    man_id = (
                SELECT
                        itf.man_id
                    FROM
                        insis_cust.intrf_lpv_people_ids itf
                    WHERE
                        itf.insunix_code IN(
                            SELECT
                                policy_holder_code
                            FROM cust_migration.lpv_sctr_master_stg dat
                            WHERE
                                dat.ctrl_id = 43795
                            )
                )
;

SELECT *
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
                            cust_migration.fa_migr_sctr_err ctrl
                        WHERE
                            stag_id	= 0
                            AND errseq	= 0
                            AND errtype = 'REP' --record ready for report
                            AND EXISTS (SELECT 1 
                                        FROM cust_migration.FA_MIGR_SCTR_STG stg
                                        WHERE stg.control_id = ctrl.control_id
                                        AND stg.att_status IN ('2','3'))
                        ORDER BY
                            control_id ASC
                    )
                WHERE
                    ROWNUM = 1
            );









--===============================================================================
INSERT INTO FA_MIGR_SCTR_STG 
SELECT * FROM lpv_sctr_master_stg WHERE ctrl_id = 44290;


--===============================================================================

DECLARE
  SYS_CTRL_ID NUMBER := 44290; --43871;
  FILE_ID NUMBER := 3090019802;
  FILE_NAME VARCHAR2(200) := 'UPLOAD SCTR_TRAMA FINAL.20200408.fmh.xlsx';
  POLLER_NAME VARCHAR2(200) := 'XLS_UPLOAD_SCTR';
BEGIN

    --deletes previuos report record
--  DELETE cust_migration.lpv_sctr_master_ctrl
--  WHERE batch_id = SYS_CTRL_ID;

  DELETE cust_migration.FA_MIGR_SCTR_ERR
  WHERE  control_id = SYS_CTRL_ID 
  ;
  
  DELETE cust_migration.sta_log
    WHERE table_name = 'FA_CUST_MIGR_SCTR'
    AND batch_id LIKE SYS_CTRL_ID ||'%';


  COMMIT;

  FA_CUST_MIGR_SCTR.sctr_wrapper(
                    pi_control_id => SYS_CTRL_ID,
                    pi_FILE_ID     => FILE_ID,
                    pi_FILE_NAME   => FILE_NAME,
                    pi_POLLER_NAME => POLLER_NAME
  );

--rollback; 

END;

--
--DECLARE
--  SYS_CTRL_ID NUMBER := 44290; --43871;
--  FILE_ID NUMBER := 3090019802;
--  FILE_NAME VARCHAR2(200) := 'UPLOAD SCTR_TRAMA FINAL.20200408.fmh.xlsx';
--  POLLER_NAME VARCHAR2(200) := 'XLS_UPLOAD_SCTR';
--BEGIN
--
--    --deletes previuos report record
--  DELETE cust_migration.lpv_sctr_master_ctrl
--  WHERE batch_id = SYS_CTRL_ID;
--
--  DELETE cust_migration.lpv_sctr_master_error_log
--  WHERE  sys_poller_process_ctrl_id = SYS_CTRL_ID;
--  
--    update insis_cust_lpv.sys_poller_process_ctrl
--    set status = 'PREPARED'
--    where poller_name like 'XLS_UPLOAD_SCTR%' 
--    and sys_poller_process_ctrl_id = SYS_CTRL_ID;
--
--  delete cust_migration.sta_log
--    WHERE table_name = 'LPV_SCTR_MASTER'
--    and batch_id like SYS_CTRL_ID ||'%';
--
--
--  COMMIT;
--
--  LPV_SCTR_MASTER.LPV_SCTR_MASTER_WRAPPER(
--                    SYS_CTRL_ID => SYS_CTRL_ID,
--                    FILE_ID => FILE_ID,
--                    FILE_NAME => FILE_NAME,
--                    POLLER_NAME => POLLER_NAME
--  );
--
----rollback; 
--
--END;

--recuperar reporte a generar
DECLARE
  PO_POLLER_ID NUMBER;
  PO_FILE_NAME VARCHAR2(200);
  PO_DATE_INI DATE;
  PO_DATE_END DATE;
  PO_STATUS VARCHAR2(200);
  PO_POLLER_NAME VARCHAR2(200);
  PO_SUCCESS_FLAG NUMBER;
BEGIN

  LPV_SCTR_MASTER.GET_LPV_SCTR_MASTER_MASS_RESULT(
    PO_POLLER_ID => PO_POLLER_ID,
    PO_FILE_NAME => PO_FILE_NAME,
    PO_DATE_INI => PO_DATE_INI,
    PO_DATE_END => PO_DATE_END,
    PO_STATUS => PO_STATUS,
    PO_POLLER_NAME => PO_POLLER_NAME,
    PO_SUCCESS_FLAG => PO_SUCCESS_FLAG
  );
    DBMS_OUTPUT.PUT_LINE('PO_POLLER_ID = ' || PO_POLLER_ID ||','||PO_POLLER_NAME);
END;


DECLARE
  ERR_SYS_CTRL_ID NUMBER;
  FILE_ID NUMBER;
  BATCH_ID NUMBER;
BEGIN
  ERR_SYS_CTRL_ID := NULL;
  FILE_ID := NULL;
  BATCH_ID := NULL;

  LPV_SCTR_MASTER.UPDATE_SCTR_MASTER_PROCESS_STATUS(
    ERR_SYS_CTRL_ID => ERR_SYS_CTRL_ID,
    FILE_ID => FILE_ID,
    BATCH_ID => BATCH_ID
  );
--rollback; 
END;


srv_error


UPDATE 
    (
    SELECT substr(policy_id, 1, 4)||substr(policy_id, 7, 6) new_polno, 
           P.policy_no actual_polno, P.*
    FROM insis_gen_v10.POLICY P
    WHERE 1=1
    AND policy_no IN (SELECT UNIQUE REPLACE(s.policy_name, ' ', '')  polno
--    and policy_id in (select unique s.att_policy_id
                        FROM cust_migration.fa_migr_sctr_stg s 
                        WHERE control_id  = (
                                        SELECT
                                                MAX(sys_poller_process_ctrl_id)
                                            FROM
                                                insis_cust_lpv.sys_poller_process_ctrl
                                            WHERE
                                                poller_name = 'XLS_MIGR_SCTR'
                                        )
                        )
--    and policy_no in ('600382','704112','800862','1242432','1245772')                    
--    and policy_no <> (substr(policy_id, 1, 4)||substr(policy_id, 7, 6))  --to_char(policy_id)
    )
    set policy_no = new_polno
    --policy_name = new_polno
    WHERE actual_polno <> new_polno