/*
   Reports country codes whose customer counts are outside the configured
   thresholds: greater than 5,000 or less than 2,000 customers.

   The script loads customer records, groups them by CountryCode, counts the
   customers in each group, and retains only countries matching either strict
   threshold. 
   
   Script output:  CountryCode,CustomerCount
*/

-- Declare path vars
%declare customersCsv '/user/cs585/prj_input/customers.csv'

customers = LOAD '$customersCsv'
    USING PigStorage(',')
    AS (
        ID:int,
        Name:chararray,
        Age:int,
        Gender:chararray,
        CountryCode:int,
        Salary:double
    );

-- Get needed Cols from customers.csv
customer_attrs = FOREACH customers
    GENERATE ID AS CustID, CountryCode;


ccGRP1 = GROUP customer_attrs BY CountryCode;

ccGRP2 = FOREACH ccGRP1
    GENERATE group AS CountryCode, COUNT(customer_attrs) AS CustomerCount;

countryCodeGroup = FILTER ccGRP2 BY
    CustomerCount > 5000 OR CustomerCount < 2000;


STORE countryCodeGroup
INTO '/user/cs585/prj_output/cc_custcounts'
USING PigStorage(',');