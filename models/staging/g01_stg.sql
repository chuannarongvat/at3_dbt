{{
    config(
        unique_key='lga_code_2016'
    )
}}

with source as (
    select * from {{ source('raw', 'census_g01') }}
),

staged_data as (
    select
        substring(lga_code_2016, '\d+') as lga_code,
        tot_p_m,
        tot_p_f,
        tot_p_p,
        age_0_4_yr_m,
        age_0_4_yr_f,
        age_0_4_yr_p,
        age_5_14_yr_m,
        age_5_14_yr_f,
        age_5_14_yr_p,
        age_15_19_yr_m,
        age_15_19_yr_f,
        age_15_19_yr_p,
        age_20_24_yr_m,
        age_20_24_yr_f,
        age_20_24_yr_p,
        age_25_34_yr_m,
        age_25_34_yr_f,
        age_25_34_yr_p,
        age_35_44_yr_m,
        age_35_44_yr_f,
        age_35_44_yr_p,
        age_45_54_yr_m,
        age_45_54_yr_f,
        age_45_54_yr_p,
        age_55_64_yr_m,
        age_55_64_yr_f,
        age_55_64_yr_p,
        age_65_74_yr_m,
        age_65_74_yr_f,
        age_65_74_yr_p,
        age_75_84_yr_m,
        age_75_84_yr_f,
        age_75_84_yr_p,
        age_85ov_m,
        age_85ov_f,
        age_85ov_p,
        counted_census_night_home_m,
        counted_census_night_home_f,
        counted_census_night_home_p,
        count_census_nt_ewhere_aust_m,
        count_census_nt_ewhere_aust_f,
        count_census_nt_ewhere_aust_p,
        indigenous_psns_aboriginal_m,
        indigenous_psns_aboriginal_f,
        indigenous_psns_aboriginal_p,
        indig_psns_torres_strait_is_m,
        indig_psns_torres_strait_is_f,
        indig_psns_torres_strait_is_p,
        indig_bth_abor_torres_st_is_m,
        indig_bth_abor_torres_st_is_f,
        indig_bth_abor_torres_st_is_p,
        indigenous_p_tot_m,
        indigenous_p_tot_f,
        indigenous_p_tot_p,
        birthplace_australia_m,
        birthplace_australia_f,
        birthplace_australia_p,
        birthplace_elsewhere_m,
        birthplace_elsewhere_f,
        birthplace_elsewhere_p,
        lang_spoken_home_eng_only_m,
        lang_spoken_home_eng_only_f,
        lang_spoken_home_eng_only_p,
        lang_spoken_home_oth_lang_m,
        lang_spoken_home_oth_lang_f,
        lang_spoken_home_oth_lang_p,
        australian_citizen_m,
        australian_citizen_f,
        australian_citizen_p,
        age_psns_att_educ_inst_0_4_m,
        age_psns_att_educ_inst_0_4_f,
        age_psns_att_educ_inst_0_4_p,
        age_psns_att_educ_inst_5_14_m,
        age_psns_att_educ_inst_5_14_f,
        age_psns_att_educ_inst_5_14_p,
        age_psns_att_edu_inst_15_19_m,
        age_psns_att_edu_inst_15_19_f,
        age_psns_att_edu_inst_15_19_p,
        age_psns_att_edu_inst_20_24_m,
        age_psns_att_edu_inst_20_24_f,
        age_psns_att_edu_inst_20_24_p,
        age_psns_att_edu_inst_25_ov_m,
        age_psns_att_edu_inst_25_ov_f,
        age_psns_att_edu_inst_25_ov_p,
        high_yr_schl_comp_yr_12_eq_m,
        high_yr_schl_comp_yr_12_eq_f,
        high_yr_schl_comp_yr_12_eq_p,
        high_yr_schl_comp_yr_11_eq_m,
        high_yr_schl_comp_yr_11_eq_f,
        high_yr_schl_comp_yr_11_eq_p,
        high_yr_schl_comp_yr_10_eq_m,
        high_yr_schl_comp_yr_10_eq_f,
        high_yr_schl_comp_yr_10_eq_p,
        high_yr_schl_comp_yr_9_eq_m,
        high_yr_schl_comp_yr_9_eq_f,
        high_yr_schl_comp_yr_9_eq_p,
        high_yr_schl_comp_yr_8_belw_m,
        high_yr_schl_comp_yr_8_belw_f,
        high_yr_schl_comp_yr_8_belw_p,
        high_yr_schl_comp_d_n_g_sch_m,
        high_yr_schl_comp_d_n_g_sch_f,
        high_yr_schl_comp_d_n_g_sch_p,
        count_psns_occ_priv_dwgs_m,
        count_psns_occ_priv_dwgs_f,
        count_psns_occ_priv_dwgs_p,
        count_persons_other_dwgs_m,
        count_persons_other_dwgs_f,
        count_persons_other_dwgs_p
    from source
)

select * from staged_data