Contains various code for producing specific output tables from the parsed DHS survey data tables. 

Most, if not all, of the functionality in the workbenches in this repository could be duplicated with SQL queries of varying complexity after the DHS data tables were loaded to a database (see DHS-DataExtraction/General for the code to do that.) However these were all written as one-off extraction requests at a point where the data were held only in CSV files.

The ACT and FeverSeekingTreatment extractions were done via FME workbenches. These extractions had relatively simple schemas (few input tables to join), but relatively complex data selection was required (e.g. mapping multiple input columns H32A-H32Z onto a single output column, dependent on the meaning of each column in a particular survey). This made FME the most pragmatic solution.

The U5M extraction required a very large output schema (~400 columns) in a flat file, to be produced by joining many input tables. However no "interpretive" processing was required. This is much more obviously a query to be done by SQL. The code in DHSTableJoiner was written for this purpose: given a csv file specifying the output columns required, it generates the SQL necessary to create the required output table by joining the input tables.

Household electricity was again a simple extraction which on this occasion was done via FME, but it could equally have been done with the TableJoiner code.