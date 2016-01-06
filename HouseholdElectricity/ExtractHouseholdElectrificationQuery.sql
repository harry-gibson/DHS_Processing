SELECT dhs_data_tables."RECH0".surveyid, dhs_data_tables."RECH0".HHID, dhs_data_tables."RECH0".HV000, dhs_data_tables."RECH0".HV001, dhs_data_tables."RECH0".HV002, dhs_data_tables."RECH0".HV005, dhs_data_tables."RECH0".HV008, dhs_data_tables."RECH0".HV009, dhs_data_tables."RECH0".HV024, dhs_data_tables."RECH0".HV025, dhs_data_tables."RECH2".HV201, dhs_data_tables."RECH2".HV206, dhs_data_tables."RECH2".HV207, dhs_data_tables."RECH2".HV208, dhs_data_tables."RECH2".HV209, dhs_data_tables."RECH2".HV210, dhs_data_tables."RECH2".HV211, dhs_data_tables."RECH2".HV212, dhs_data_tables."RECH2".HV221, dhs_data_tables."RECH2".HV270, dhs_data_tables."RECH2".HV271 
from dhs_data_tables."RECH0" LEFT OUTER JOIN dhs_data_tables."RECH2" 
ON 
dhs_data_tables."RECH0".HHID = dhs_data_tables."RECH2".HHID
and 
dhs_data_tables."RECH0".surveyid = dhs_data_tables."RECH2".surveyid
where dhs_data_tables."RECH0".surveyid != 156