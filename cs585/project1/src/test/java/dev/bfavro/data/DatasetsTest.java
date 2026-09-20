package dev.bfavro.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.junit.Test;

public class DatasetsTest {
    private static final int CUSTOMER_FIELDS = 6;
    private static final int TRANSACTION_FIELDS = 5;

    @Test
    public void createAllCustomersCreatesValidCustomersWithSequentialIds() {
        Datasets datasets = new Datasets();

        List<String> customers = datasets.createAllCustomers(100);

        assertEquals(100 * CUSTOMER_FIELDS, customers.size());
        for (int customerIndex = 0; customerIndex < 100; customerIndex++) {
            int fieldIndex = customerIndex * CUSTOMER_FIELDS;
            assertEquals(Integer.toString(customerIndex + 1), customers.get(fieldIndex));
            assertTrue(customers.get(fieldIndex + 1).matches("[A-Za-z]{10,20}"));

            int age = Integer.parseInt(customers.get(fieldIndex + 2));
            assertTrue(age >= 10 && age <= 70);
            //assertTrue(age >= 71);
            assertTrue(customers.get(fieldIndex + 3).matches("male|female"));

            int countryCode = Integer.parseInt(customers.get(fieldIndex + 4));
            assertTrue(countryCode >= 1 && countryCode <= 10);

            double salary = Double.parseDouble(customers.get(fieldIndex + 5));
            assertTrue(salary >= 100.0 && salary <= 10_000.0);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void createAllCustomersRejectsCountsAboveMaximum() {
        new Datasets().createAllCustomers(50_001);
    }

    @Test
    public void createAllCustomersSupportsMaximumCount() {
        List<String> customers = new Datasets().createAllCustomers(50_000);

        assertEquals(50_000 * CUSTOMER_FIELDS, customers.size());
        assertEquals("50000", customers.get(customers.size() - CUSTOMER_FIELDS));
    }

    @Test
    public void createAllTransactionsCreatesValidTransactionsAndCyclesCustomerIds() {
        List<String> transactions = new Datasets().createAllTransactions(50_001);

        assertEquals(50_001 * TRANSACTION_FIELDS, transactions.size());
        for (int transactionIndex = 0; transactionIndex < 50_001; transactionIndex++) {
            int fieldIndex = transactionIndex * TRANSACTION_FIELDS;
            assertEquals(Integer.toString(transactionIndex + 1), transactions.get(fieldIndex));

            int expectedCustomerId = (transactionIndex % 50_000) + 1;
            assertEquals(Integer.toString(expectedCustomerId), transactions.get(fieldIndex + 1));

            double transactionTotal = Double.parseDouble(transactions.get(fieldIndex + 2));
            assertTrue(transactionTotal >= 10.0 && transactionTotal <= 1_000.0);

            int transactionItemCount = Integer.parseInt(transactions.get(fieldIndex + 3));
            assertTrue(transactionItemCount >= 1 && transactionItemCount <= 10);
            assertTrue(transactions.get(fieldIndex + 4).matches("[A-Za-z]{20,50}"));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void createAllTransactionsRejectsCountsAboveMaximum() {
        new Datasets().createAllTransactions(5_000_001);
    }
}