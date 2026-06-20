# CMS Outpatient Claims — Data Dictionary

Generated from the deployed `bronze.cms_outpatient` table — column order, Postgres type, and
the column COMMENT are read straight from the database, so this doc mirrors what
is actually loaded. CCW storage class / length / source are joined in from
`bronze.ccw_variable_metadata`. **Do not hand-edit** — regenerate with
`scripts/build_data_dictionary.py`.

- **Columns:** 162 (30 date, 25 numeric, 107 text)
- **Source of descriptions:** CCW/NCH variable metadata ([CMS BlueButton codesets](https://github.com/CMSgov/bluebutton-csv-codesets)), applied to the table as COMMENTs by the load

| # | Column | Postgres type | CCW type | Len | Source | Description |
|--:|--------|---------------|----------|----:|--------|-------------|
| 1 | `BENE_ID` | `text` | CHAR | 15 | CCW | CCW Encrypted Beneficiary ID Number |
| 2 | `CLM_ID` | `text` | CHAR | 15 | CCW | Claim ID |
| 3 | `NCH_NEAR_LINE_REC_IDENT_CD` | `text` | CHAR | 1 | NCH | NCH Near Line Record Identification Code (RIC) |
| 4 | `NCH_CLM_TYPE_CD` | `text` | CHAR | 2 | NCH | NCH Claim Type Code |
| 5 | `CLM_FROM_DT` | `date` | DATE | 8 | NCH | Claim From Date |
| 6 | `CLM_THRU_DT` | `date` | DATE | 8 | NCH | Claim Through Date |
| 7 | `NCH_WKLY_PROC_DT` | `date` | DATE | 8 | NCH | NCH Weekly Claim Processing Date |
| 8 | `FI_CLM_PROC_DT` | `date` | DATE | 8 | NCH | FI Claim Process Date |
| 9 | `CLAIM_QUERY_CODE` | `text` | CHAR | 1 | NCH | Claim Query Code |
| 10 | `PRVDR_NUM` | `text` | CHAR | 6 |  | Provider Number |
| 11 | `CLM_FAC_TYPE_CD` | `text` | CHAR | 1 | NCH | Claim Facility Type Code |
| 12 | `CLM_SRVC_CLSFCTN_TYPE_CD` | `text` | CHAR | 1 | NCH | Claim Service Classification Type Code |
| 13 | `CLM_FREQ_CD` | `text` | CHAR | 1 | NCH | Claim Frequency Code |
| 14 | `FI_NUM` | `text` | CHAR | 5 | NCH | FI or MAC Number |
| 15 | `CLM_MDCR_NON_PMT_RSN_CD` | `text` | CHAR | 2 | NCH | Claim Medicare Non Payment Reason Code |
| 16 | `CLM_PMT_AMT` | `numeric` | NUM | 12 | NCH | Claim (Medicare) Payment Amount |
| 17 | `NCH_PRMRY_PYR_CLM_PD_AMT` | `numeric` | NUM | 12 | NCH | NCH Primary Payer (if not Medicare) Claim Paid Amount |
| 18 | `NCH_PRMRY_PYR_CD` | `text` | CHAR | 1 | NCH | NCH Primary Payer Code (if not Medicare) |
| 19 | `PRVDR_STATE_CD` | `text` | CHAR | 2 | NCH | NCH Provider SSA State Code |
| 20 | `ORG_NPI_NUM` | `text` | CHAR | 10 | NCH | Organization (or group) NPI Number |
| 21 | `AT_PHYSN_UPIN` | `text` | CHAR | 6 | NCH | Claim Attending Physician UPIN Number |
| 22 | `AT_PHYSN_NPI` | `text` | CHAR | 10 | NCH | Claim Attending Physician NPI Number |
| 23 | `OP_PHYSN_UPIN` | `text` | CHAR | 6 | NCH | Claim Operating Physician UPIN Number |
| 24 | `OP_PHYSN_NPI` | `text` | CHAR | 10 | NCH | Claim Operating Physician NPI Number |
| 25 | `OT_PHYSN_UPIN` | `text` | CHAR | 6 | NCH | Claim Other Physician UPIN Number |
| 26 | `OT_PHYSN_NPI` | `text` | CHAR | 10 | NCH | Claim Other Physician NPI Number |
| 27 | `CLM_MCO_PD_SW` | `text` | CHAR | 1 | NCH | Claim MCO Paid Switch |
| 28 | `PTNT_DSCHRG_STUS_CD` | `text` | CHAR | 2 | NCH | Patient Discharge Status Code |
| 29 | `CLM_TOT_CHRG_AMT` | `numeric` | NUM | 12 | NCH | Claim Total Charge Amount |
| 30 | `NCH_BENE_BLOOD_DDCTBL_LBLTY_AM` | `numeric` | NUM | 12 | NCH QA PROCESS | NCH Beneficiary Blood Deductible Liability Amount |
| 31 | `NCH_PROFNL_CMPNT_CHRG_AMT` | `numeric` | NUM | 12 | NCH QA Process | Professional Component Charge Amount |
| 32 | `PRNCPAL_DGNS_CD` | `text` | CHAR | 7 | NCH | Claim Principal Diagnosis Code |
| 33 | `ICD_DGNS_CD1` | `text` | CHAR | 7 | NCH | Claim Diagnosis Code I |
| 34 | `ICD_DGNS_CD2` | `text` | CHAR | 7 | NCH | Claim Diagnosis Code II |
| 35 | `ICD_DGNS_CD3` | `text` | CHAR | 7 | NCH | Claim Diagnosis Code III |
| 36 | `ICD_DGNS_CD4` | `text` | CHAR | 7 | NCH | Claim Diagnosis Code IV |
| 37 | `ICD_DGNS_CD5` | `text` | CHAR | 7 | NCH | Claim Diagnosis Code V |
| 38 | `ICD_DGNS_CD6` | `text` | CHAR | 7 | NCH | Claim Diagnosis Code VI |
| 39 | `ICD_DGNS_CD7` | `text` | CHAR | 7 | NCH | Claim Diagnosis Code VII |
| 40 | `ICD_DGNS_CD8` | `text` | CHAR | 7 | NCH | Claim Diagnosis Code VIII |
| 41 | `ICD_DGNS_CD9` | `text` | CHAR | 7 | NCH | Claim Diagnosis Code IX |
| 42 | `ICD_DGNS_CD10` | `text` | CHAR | 7 | NCH | Claim Diagnosis Code X |
| 43 | `ICD_DGNS_CD11` | `text` | CHAR | 7 | NCH | Claim Diagnosis Code XI |
| 44 | `ICD_DGNS_CD12` | `text` | CHAR | 7 | NCH | Claim Diagnosis Code XII |
| 45 | `ICD_DGNS_CD13` | `text` | CHAR | 7 | NCH | Claim Diagnosis Code XIII |
| 46 | `ICD_DGNS_CD14` | `text` | CHAR | 7 | NCH | Claim Diagnosis Code XIV |
| 47 | `ICD_DGNS_CD15` | `text` | CHAR | 7 | NCH | Claim Diagnosis Code XV |
| 48 | `ICD_DGNS_CD16` | `text` | CHAR | 7 | NCH | Claim Diagnosis Code XVI |
| 49 | `ICD_DGNS_CD17` | `text` | CHAR | 7 | NCH | Claim Diagnosis Code XVII |
| 50 | `ICD_DGNS_CD18` | `text` | CHAR | 7 | NCH | Claim Diagnosis Code XVIII |
| 51 | `ICD_DGNS_CD19` | `text` | CHAR | 7 | NCH | Claim Diagnosis Code XIX |
| 52 | `ICD_DGNS_CD20` | `text` | CHAR | 7 | NCH | Claim Diagnosis Code XX |
| 53 | `ICD_DGNS_CD21` | `text` | CHAR | 7 | NCH | Claim Diagnosis Code XXI |
| 54 | `ICD_DGNS_CD22` | `text` | CHAR | 7 | NCH | Claim Diagnosis Code XXII |
| 55 | `ICD_DGNS_CD23` | `text` | CHAR | 7 | NCH | Claim Diagnosis Code XXIII |
| 56 | `ICD_DGNS_CD24` | `text` | CHAR | 7 | NCH | Claim Diagnosis Code XXIV |
| 57 | `ICD_DGNS_CD25` | `text` | CHAR | 7 | NCH | Claim Diagnosis Code XXV |
| 58 | `FST_DGNS_E_CD` | `text` | CHAR | 7 | NCH | First Claim Diagnosis E Code |
| 59 | `ICD_DGNS_E_CD1` | `text` | CHAR | 7 | NCH | Claim Diagnosis E Code I |
| 60 | `ICD_DGNS_E_CD2` | `text` | CHAR | 7 | NCH | Claim Diagnosis E Code II |
| 61 | `ICD_DGNS_E_CD3` | `text` | CHAR | 7 | NCH | Claim Diagnosis E Code III |
| 62 | `ICD_DGNS_E_CD4` | `text` | CHAR | 7 | NCH | Claim Diagnosis E Code IV |
| 63 | `ICD_DGNS_E_CD5` | `text` | CHAR | 7 | NCH | Claim Diagnosis E Code V |
| 64 | `ICD_DGNS_E_CD6` | `text` | CHAR | 7 | NCH | Claim Diagnosis E Code VI |
| 65 | `ICD_DGNS_E_CD7` | `text` | CHAR | 7 | NCH | Claim Diagnosis E Code VII |
| 66 | `ICD_DGNS_E_CD8` | `text` | CHAR | 7 | NCH | Claim Diagnosis E Code VIII |
| 67 | `ICD_DGNS_E_CD9` | `text` | CHAR | 7 | NCH | Claim Diagnosis E Code IX |
| 68 | `ICD_DGNS_E_CD10` | `text` | CHAR | 7 | NCH | Claim Diagnosis E Code X |
| 69 | `ICD_DGNS_E_CD11` | `text` | CHAR | 7 | NCH | Claim Diagnosis E Code XI |
| 70 | `ICD_DGNS_E_CD12` | `text` | CHAR | 7 | NCH | Claim Diagnosis E Code XII |
| 71 | `ICD_PRCDR_CD1` | `text` | CHAR | 7 | NCH | Claim Procedure Code I |
| 72 | `PRCDR_DT1` | `date` | DATE | 8 | NCH | Claim Procedure Code I Date |
| 73 | `ICD_PRCDR_CD2` | `text` | CHAR | 7 | NCH | Claim Procedure Code II |
| 74 | `PRCDR_DT2` | `date` | DATE | 8 | NCH | Claim Procedure Code II Date |
| 75 | `ICD_PRCDR_CD3` | `text` | CHAR | 7 | NCH | Claim Procedure Code III |
| 76 | `PRCDR_DT3` | `date` | DATE | 8 | NCH | Claim Procedure Code III Date |
| 77 | `ICD_PRCDR_CD4` | `text` | CHAR | 7 | NCH | Claim Procedure Code IV |
| 78 | `PRCDR_DT4` | `date` | DATE | 8 | NCH | Claim Procedure Code IV Date |
| 79 | `ICD_PRCDR_CD5` | `text` | CHAR | 7 | NCH | Claim Procedure Code V |
| 80 | `PRCDR_DT5` | `date` | DATE | 8 | NCH | Claim Procedure Code V Date |
| 81 | `ICD_PRCDR_CD6` | `text` | CHAR | 7 | NCH | Claim Procedure Code VI |
| 82 | `PRCDR_DT6` | `date` | DATE | 8 | NCH | Claim Procedure Code VI Date |
| 83 | `ICD_PRCDR_CD7` | `text` | CHAR | 7 | NCH | Claim Procedure Code VII |
| 84 | `PRCDR_DT7` | `date` | DATE | 8 | NCH | Claim Procedure Code VII Date |
| 85 | `ICD_PRCDR_CD8` | `text` | CHAR | 7 | NCH | Claim Procedure Code VIII |
| 86 | `PRCDR_DT8` | `date` | DATE | 8 | NCH | Claim Procedure Code VIII Date |
| 87 | `ICD_PRCDR_CD9` | `text` | CHAR | 7 | NCH | Claim Procedure Code IX |
| 88 | `PRCDR_DT9` | `date` | DATE | 8 | NCH | Claim Procedure Code IX Date |
| 89 | `ICD_PRCDR_CD10` | `text` | CHAR | 7 | NCH | Claim Procedure Code X |
| 90 | `PRCDR_DT10` | `date` | DATE | 8 | NCH | Claim Procedure Code X Date |
| 91 | `ICD_PRCDR_CD11` | `text` | CHAR | 7 | NCH | Claim Procedure Code XI |
| 92 | `PRCDR_DT11` | `date` | DATE | 8 | NCH | Claim Procedure Code XI Date |
| 93 | `ICD_PRCDR_CD12` | `text` | CHAR | 7 | NCH | Claim Procedure Code XII |
| 94 | `PRCDR_DT12` | `date` | DATE | 8 | NCH | Claim Procedure Code XII Date |
| 95 | `ICD_PRCDR_CD13` | `text` | CHAR | 7 | NCH | Claim Procedure Code XIII |
| 96 | `PRCDR_DT13` | `date` | DATE | 8 | NCH | Claim Procedure Code XIII Date |
| 97 | `ICD_PRCDR_CD14` | `text` | CHAR | 7 | NCH | Claim Procedure Code XIV |
| 98 | `PRCDR_DT14` | `date` | DATE | 8 | NCH | Claim Procedure Code XIV Date |
| 99 | `ICD_PRCDR_CD15` | `text` | CHAR | 7 | NCH | Claim Procedure Code XV |
| 100 | `PRCDR_DT15` | `date` | DATE | 8 | NCH | Claim Procedure Code XV Date |
| 101 | `ICD_PRCDR_CD16` | `text` | CHAR | 7 | NCH | Claim Procedure Code XVI |
| 102 | `PRCDR_DT16` | `date` | DATE | 8 | NCH | Claim Procedure Code XVI Date |
| 103 | `ICD_PRCDR_CD17` | `text` | CHAR | 7 | NCH | Claim Procedure Code XVII |
| 104 | `PRCDR_DT17` | `date` | DATE | 8 | NCH | Claim Procedure Code XVII Date |
| 105 | `ICD_PRCDR_CD18` | `text` | CHAR | 7 | NCH | Claim Procedure Code XVIII |
| 106 | `PRCDR_DT18` | `date` | DATE | 8 | NCH | Claim Procedure Code XVIII Date |
| 107 | `ICD_PRCDR_CD19` | `text` | CHAR | 7 | NCH | Claim Procedure Code XIX |
| 108 | `PRCDR_DT19` | `date` | DATE | 8 | NCH | Claim Procedure Code XIX Date |
| 109 | `ICD_PRCDR_CD20` | `text` | CHAR | 7 | NCH | Claim Procedure Code XX |
| 110 | `PRCDR_DT20` | `date` | DATE | 8 | NCH | Claim Procedure Code XX Date |
| 111 | `ICD_PRCDR_CD21` | `text` | CHAR | 7 | NCH | Claim Procedure Code XXI |
| 112 | `PRCDR_DT21` | `date` | DATE | 8 | NCH | Claim Procedure Code XXI Date |
| 113 | `ICD_PRCDR_CD22` | `text` | CHAR | 7 | NCH | Claim Procedure Code XXII |
| 114 | `PRCDR_DT22` | `date` | DATE | 8 | NCH | Claim Procedure Code XXII Date |
| 115 | `ICD_PRCDR_CD23` | `text` | CHAR | 7 | NCH | Claim Procedure Code XXIII |
| 116 | `PRCDR_DT23` | `date` | DATE | 8 | NCH | Claim Procedure Code XXIII Date |
| 117 | `ICD_PRCDR_CD24` | `text` | CHAR | 7 | NCH | Claim Procedure Code XXIV |
| 118 | `PRCDR_DT24` | `date` | DATE | 8 | NCH | Claim Procedure Code XXIV Date |
| 119 | `ICD_PRCDR_CD25` | `text` | CHAR | 7 | NCH | Claim Procedure Code XXV |
| 120 | `PRCDR_DT25` | `date` | DATE | 8 | NCH | Claim Procedure Code XXV Date |
| 121 | `RSN_VISIT_CD1` | `text` | CHAR | 7 | NCH | Reason for Visit Diagnosis Code I |
| 122 | `RSN_VISIT_CD2` | `text` | CHAR | 7 | NCH | Reason for Visit Diagnosis Code II |
| 123 | `RSN_VISIT_CD3` | `text` | CHAR | 7 | NCH | Reason for Visit Diagnosis Code III |
| 124 | `NCH_BENE_PTB_DDCTBL_AMT` | `numeric` | NUM | 12 | NCH QA PROCESS | NCH Beneficiary Part B Deductible Amount |
| 125 | `NCH_BENE_PTB_COINSRNC_AMT` | `numeric` | NUM | 12 | NCH QA PROCESS | NCH Beneficiary Part B Coinsurance Amount |
| 126 | `CLM_OP_PRVDR_PMT_AMT` | `numeric` | NUM | 12 | NCH | Claim Outpatient Provider Payment Amount |
| 127 | `CLM_OP_BENE_PMT_AMT` | `numeric` | NUM | 12 | NCH | Claim Outpatient Payment Amount to Beneficiary |
| 128 | `CLM_LINE_NUM` | `numeric` | NUM | 13 | CCW | Claim Line Number |
| 129 | `REV_CNTR` | `text` | CHAR | 4 | NCH | Revenue Center Code |
| 130 | `REV_CNTR_DT` | `date` | DATE | 8 | NCH | Revenue Center Date |
| 131 | `REV_CNTR_1ST_ANSI_CD` | `text` | CHAR | 5 | NCH | Revenue Center 1st ANSI Code |
| 132 | `REV_CNTR_2ND_ANSI_CD` | `text` | CHAR | 5 | NCH | Revenue Center 2nd ANSI Code |
| 133 | `REV_CNTR_3RD_ANSI_CD` | `text` | CHAR | 5 | NCH | Revenue Center 3rd ANSI Code |
| 134 | `REV_CNTR_4TH_ANSI_CD` | `text` | CHAR | 5 | NCH | Revenue Center 4th ANSI Code |
| 135 | `REV_CNTR_APC_HIPPS_CD` | `text` | CHAR | 5 | NCH | Revenue Center APC or HIPPS Code |
| 136 | `HCPCS_CD` | `text` | CHAR | 5 | NCH | Healthcare Common Procedure Coding System (HCPCS) Code |
| 137 | `HCPCS_1ST_MDFR_CD` | `text` | CHAR | 5 | NCH | HCPCS Initial Modifier Code |
| 138 | `HCPCS_2ND_MDFR_CD` | `text` | CHAR | 5 | NCH | HCPCS Second Modifier Code |
| 139 | `REV_CNTR_PMT_MTHD_IND_CD` | `text` | CHAR | 2 | NCH | Revenue Center Payment Method Indicator Code |
| 140 | `REV_CNTR_DSCNT_IND_CD` | `text` | CHAR | 1 | NCH | Revenue Center Discount Indicator Code |
| 141 | `REV_CNTR_PACKG_IND_CD` | `text` | CHAR | 1 | NCH | Revenue Center Packaging Indicator Code |
| 142 | `REV_CNTR_OTAF_PMT_CD` | `text` | CHAR | 1 | NCH | Revenue Center Obligation to Accept As Full (OTAF) Payment Code |
| 143 | `REV_CNTR_IDE_NDC_UPC_NUM` | `text` | CHAR | 24 | NCH | Revenue Center IDE, NDC, or UPC Number |
| 144 | `REV_CNTR_UNIT_CNT` | `numeric` | NUM | 8 | NCH | Revenue Center Unit Count |
| 145 | `REV_CNTR_RATE_AMT` | `numeric` | NUM | 12 | NCH | Revenue Center Rate Amount |
| 146 | `REV_CNTR_BLOOD_DDCTBL_AMT` | `numeric` | NUM | 12 | NCH | Revenue Center Blood Deductible Amount |
| 147 | `REV_CNTR_CASH_DDCTBL_AMT` | `numeric` | NUM | 12 | NCH | Revenue Center Cash Deductible Amount |
| 148 | `REV_CNTR_COINSRNC_WGE_ADJSTD_C` | `numeric` | NUM | 12 | NCH | Revenue Center Coinsurance/Wage Adjusted Coinsurance Amount |
| 149 | `REV_CNTR_RDCD_COINSRNC_AMT` | `numeric` | NUM | 12 | NCH | Revenue Center Reduced Coinsurance Amount |
| 150 | `REV_CNTR_1ST_MSP_PD_AMT` | `numeric` | NUM | 12 | NCH | Revenue Center 1st Medicare Secondary Payer (MSP) Paid Amount |
| 151 | `REV_CNTR_2ND_MSP_PD_AMT` | `numeric` | NUM | 12 | NCH | Revenue Center 2nd Medicare Secondary Payer (MSP) Paid Amount |
| 152 | `REV_CNTR_PRVDR_PMT_AMT` | `numeric` | NUM | 12 | NCH | Revenue Center (Medicare) Provider Payment Amount |
| 153 | `REV_CNTR_BENE_PMT_AMT` | `numeric` | NUM | 12 | NCH | Revenue Center Payment Amount to Beneficiary |
| 154 | `REV_CNTR_PTNT_RSPNSBLTY_PMT` | `numeric` | NUM | 12 | NCH | Revenue Center Patient Responsibility Payment Amount |
| 155 | `REV_CNTR_PMT_AMT_AMT` | `numeric` | NUM | 12 | NCH | Revenue Center (Medicare) Payment Amount |
| 156 | `REV_CNTR_TOT_CHRG_AMT` | `numeric` | NUM | 12 | NCH | Revenue Center Total Charge Amount |
| 157 | `REV_CNTR_NCVRD_CHRG_AMT` | `numeric` | NUM | 12 | NCH | Revenue Center Non-Covered Charge Amount |
| 158 | `REV_CNTR_STUS_IND_CD` | `text` | CHAR | 2 | NCH | Revenue Center Status Indicator Code |
| 159 | `REV_CNTR_NDC_QTY` | `numeric` | NUM | 10 | NCH | Revenue Center National Drug Code (NDC) Quantity |
| 160 | `REV_CNTR_NDC_QTY_QLFR_CD` | `text` | CHAR | 2 | NCH | Revenue Center NDC Quantity Qualifier Code |
| 161 | `RNDRNG_PHYSN_UPIN` | `text` | CHAR | 12 | NCH | Revenue Center Rendering Physician UPIN |
| 162 | `RNDRNG_PHYSN_NPI` | `text` | CHAR | 12 | NCH | Rendering Physician NPI |
