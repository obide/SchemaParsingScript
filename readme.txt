SU_EBS_BOM
SU_EBS_EAM
SU_EBS_FND
SU_EBS_HR
SU_EBS_INV
SU_EBS_PO
SU_EBS_WIP
SU_EBS_XXDL

It must be located as sibling to incorta.py -the command line tool as I am using parts of its functionality. You provide the server URL, tenant name, schema patter, table pattern, column pattern, data type to change and to which value.

The patterns works as normal SQL like statements (e.g. %_CODE will match all items with suffix _code). It applies to schemas, tables and columns. 

There's an optional last parameter (value true) to change the matching column to dimension. You may pass that param as follows
I


python list_incremental.py http://dev01.incorta.com:8080/incorta development hsellami stanford123 EBS% true