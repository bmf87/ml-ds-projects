/*
   Reports every customer tied for the minimum number of transactions.

   The script joins customer details to transaction records by CustID, counts
   transactions for each customer, finds the global minimum count, and filters
   the per-customer results to retain every customer tied at that minimum.
   
   Script output: CustomerName,NumberOfTransactions

   The customer ID remains part of the grouping key so customers with the same
   name are counted independently.
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
    GENERATE ID AS CustID, Name;

transactions = LOAD '$transactionsCsv'
    USING PigStorage(',')
    AS (
        TransID:int,
        CustID:int,
        TransTotal:double,
        TransNumItems:int,
        TransDesc:chararray
    );

-- Get needed Cols from transactions.csv
transaction_details = FOREACH transactions
    GENERATE CustID;

-- JOIN customer with transactions on CustID
joined = JOIN customer_details BY CustID, transaction_details BY CustID;

-- Group by ID, Name so duplicate names remain distinct customers
grouped_customers = GROUP joined BY (
    customer_details::CustID,
    customer_details::Name
);

-- Count transactions for each customer
-- Flattens out (CustID, Name) tuple
customer_counts = FOREACH grouped_customers
    GENERATE
        FLATTEN(group) AS (CustID, CustomerName),
        COUNT(joined) AS TransCount;

-- Creates 1x big group containing bag of { all rows }
-- Enables the global MIN calculation 
global_count = GROUP customer_counts ALL;

-- Global min as MinTransCount
minimum_count = FOREACH global_count
    GENERATE MIN(customer_counts.TransCount) AS MinTransCount;

-- Cross Join: Adds minimum_count to every row
with_minimum = CROSS customer_counts, minimum_count;

least_transaction_customers = FILTER with_minimum BY
    customer_counts::TransCount == minimum_count::MinTransCount;

final_output = FOREACH least_transaction_customers
    GENERATE
        customer_counts::CustomerName AS CustomerName,
        customer_counts::TransCount AS NumberOfTransactions;

STORE final_output
INTO '/user/cs585/prj_output/cust_mintranscount'
USING PigStorage(',');
