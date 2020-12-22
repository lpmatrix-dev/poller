    --------------------------------------------------------------------------------
    -- Name: CUST_MIGRATION__SCTR_MIGR_DCL
    -------------------------------------
    -- Purpose: Grants to cust_migration for use of INSIS' poller 
    -- Type: Script
    -- Versioning:
    --     LPV-FRAMEND0     2020-03-11      creation
    --     LPV-FRAMEND0     2020-04-14      ISS040-Objects standarization 
    ---------------------------------------------------------------------------------

GRANT SELECT ON cust_migration.FA_MIGR_SCTR_STG TO insis_gen_v10_rls;
GRANT SELECT ON cust_migration.FA_MIGR_SCTR_ERR TO insis_gen_v10_rls;
GRANT EXECUTE ON cust_migration.FA_CUST_MIGR_SCTR TO insis_gen_v10_rls;


--grant references on insis_cust_lpv.sys_poller_process_ctrl TO cust_migration;
--grant execute on insis_cust_lpv.sys_schema_utils to cust_migration;
--grant update on insis_cust_lpv.sys_poller_process_ctrl TO cust_migration;
--grant execute on insis_sys_v10.srv_error to cust_migration; login with insis_sys_v10
--grant execute on dbms_scheduler to cust_migration;
--grant create job to cust_migration;