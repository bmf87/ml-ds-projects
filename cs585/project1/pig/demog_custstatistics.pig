/*
   Summarizes transaction totals by customer age range and gender.

   The script joins customer demographics to transactions by CustID, assigns
   each transaction to one of six age ranges and one gender group, then reports
   the minimum, maximum, and average TransTotal for each group. 
   
   Output contains:

       AgeRange,Gender,MinTransTotal,MaxTransTotal,AvgTransTotal

   The age ranges match AgeRangeSummary.java:
       [10,20), [20,30), [30,40), [40,50), [50,60), and [60,70]
*/

-- Declare path vars
%declare customersCsv '/user/cs585/prj_input/customers.csv'
%declare transactionsCsv '/user/cs585/prj_input/transactions.csv'

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
customer_details = FOREACH customers
    GENERATE ID AS CustID, Age, Gender;

transactions = LOAD '$transactionsCsv'
    USING PigStorage(',')
    AS (
        TransID:int,
        CustID:int,
        TransTotal:double,
        TransNumItems:int,
        TransDesc:chararray
    );

-- Keep only Tx fields needed for final aggregates
transaction_details = FOREACH transactions
    GENERATE CustID, TransTotal;

joined = JOIN customer_details BY CustID, transaction_details BY CustID;

-- Using age labels from AgeRangeSummary.java
-- Generate ageRange,gender,Transtotal tuple
age_gender_transactions = FOREACH joined
    GENERATE
        (CASE
            WHEN customer_details::Age < 20 THEN '[10,20)'
            WHEN customer_details::Age < 30 THEN '[20,30)'
            WHEN customer_details::Age < 40 THEN '[30,40)'
            WHEN customer_details::Age < 50 THEN '[40,50)'
            WHEN customer_details::Age < 60 THEN '[50,60)'
            ELSE '[60,70]'
        END) AS AgeRange,
        customer_details::Gender AS Gender,
        transaction_details::TransTotal AS TransTotal;

-- Group Tx totals by required demographic dimensions
-- Pig requires a tuple to group by multiple elements: (AgeRange, Gender)
grouped_demographics = GROUP age_gender_transactions BY (AgeRange, Gender);

summary = FOREACH grouped_demographics
    GENERATE
        FLATTEN(group) AS (AgeRange, Gender),
        ROUND_TO(MIN(age_gender_transactions.TransTotal), 2) AS MinTransTotal,
        ROUND_TO(MAX(age_gender_transactions.TransTotal), 2) AS MaxTransTotal,
        ROUND_TO(AVG(age_gender_transactions.TransTotal), 2) AS AvgTransTotal;

STORE summary
INTO '/user/cs585/prj_output/demog_custstatistics'
USING PigStorage(',');
