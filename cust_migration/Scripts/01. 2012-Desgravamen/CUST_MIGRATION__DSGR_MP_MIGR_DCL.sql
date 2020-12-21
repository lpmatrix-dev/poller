grant execute on cust_migration.fa_cust_migr_dsgr_mp to insis_gen_v10_rls;

grant select on cust_migration.fa_migr_dsgr_mp_pol to insis_gen_v10_rls;

grant select on cust_migration.fa_migr_dsgr_mp_cov to insis_gen_v10_rls;

grant select, update on cust_migration.fa_migr_poller_err to insis_gen_v10_rls;

--desde insis_cust
grant execute on insis_cust.lpv_commval_obj_lvl_type to cust_migration;
