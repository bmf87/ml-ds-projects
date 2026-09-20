package dev.bfavro.data;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates bounded synthetic customer and transaction datasets for the
 * MapReduce jobs in this project. Records are represented as flat lists of CSV
 * fields and written as fixed-width rows to the configured files under
 * {@value #OUTPUT_DIR}.
 *
 * <p>Customer and transaction IDs are sequential, while the remaining fields
 * follow their defined random ranges. Transaction customer IDs cycle through
 * the available customer ID range to provide an even distribution.</p>
 */
public class Datasets {
    public static final String OUTPUT_DIR = "data/";
    public static final String CUSTOMER_CSV = "customers.csv";
    public static final String TRANSACTION_CSV = "transactions.csv";
    public static enum DatasetType {
        CUSTOMER,
        TRANSACTION
    }
    public static final int MAX_CUSTOMERS = 50_000;
    public static final int MAX_TRANSACTIONS = 5_000_000;
    public static final int CUSTOMER_FIELDS = 6;
    public static final int TRANSACTION_FIELDS = 5;
    private static final String NAME_CHARACTERS =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    private String getOutputFilePath(DatasetType dsType) {
        switch(dsType) {
            case CUSTOMER:
                return OUTPUT_DIR + CUSTOMER_CSV;
            case TRANSACTION:
                return OUTPUT_DIR + TRANSACTION_CSV;
            default:
                throw new IllegalArgumentException("Unknown dataset type: " + dsType);
        }
    }
    
     // Logic to create a dataset based on enum type, data list, and row item count
    private void writeDataset(DatasetType dsType, List<String> data, int rowItems) {
        try {
            FileWriter writer = new FileWriter(getOutputFilePath(dsType));
            PrintWriter printWriter = new PrintWriter(writer);
            String csvLine = "";
            int itemCount = 1;
            for (String dataItem : data) {
                //System.out.println("Processing data item: " + itemCount + ") " + dataItem);
                if (itemCount != rowItems) {
                    csvLine += dataItem + ",";
                    itemCount++;
                }
                else { // Start new line every numItems
                    csvLine += dataItem ;
                    //System.out.println("Starting new line with: " + csvLine);
                    printWriter.println(csvLine);
                    csvLine = "";
                    itemCount = 1; // Reset item count for new line
                }
            }
            // Print any remaining data in csvLine
            if (!csvLine.isEmpty()) {
                printWriter.println(csvLine);
            }
            printWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public List<String> createAllCustomers(int customerCount) {
        if (customerCount < 0 || customerCount > MAX_CUSTOMERS) {
            throw new IllegalArgumentException(
                "customerCount must be between 0 and " + MAX_CUSTOMERS);
        }

        List<String> customers = new ArrayList<>(customerCount * CUSTOMER_FIELDS);
        for (int customerId = 1; customerId <= customerCount; customerId++) {
            customers.addAll(createCustomer(customerId));
        }
        return customers;
    }

    private List<String> createCustomer(int customerId) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int nameLength = random.nextInt(10, 21);
        StringBuilder name = new StringBuilder(nameLength);
        // Generate random name
        for (int characterIndex = 0; characterIndex < nameLength; characterIndex++) {
            name.append(NAME_CHARACTERS.charAt(random.nextInt(NAME_CHARACTERS.length())));
        }

        List<String> customer = new ArrayList<>(CUSTOMER_FIELDS);
        customer.add(Integer.toString(customerId));
        customer.add(name.toString());
        // age
        customer.add(Integer.toString(random.nextInt(10, 71)));
        // gender (male or female)
        customer.add(random.nextBoolean() ? "male" : "female");
        // country code (1 to 10)
        customer.add(Integer.toString(random.nextInt(1, 11)));
        // salary (100.0 to 10_000.0)
        customer.add(String.format(Locale.US, "%.2f", random.nextDouble(100.0, 10_000.0)));
        return customer;
    }

    public List<String> createAllTransactions(int transactionCount) {
        if (transactionCount < 0 || transactionCount > MAX_TRANSACTIONS) {
            throw new IllegalArgumentException(
                "transactionCount must be between 0 and " + MAX_TRANSACTIONS);
        }

        List<String> transactions = new ArrayList<>(transactionCount * TRANSACTION_FIELDS);
        for (int transactionId = 1; transactionId <= transactionCount; transactionId++) {
            transactions.addAll(createTransaction(transactionId));
        }
        return transactions;
    }

    private List<String> createTransaction(int transactionId) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        // Ensure customerId cycles through the available customers
        // Balanced Distribution: TransId % MAX_CUSTOMERS = 100 TXs
        int customerId = ((transactionId - 1) % MAX_CUSTOMERS) + 1;
        int descriptionLength = random.nextInt(20, 51);
        StringBuilder description = new StringBuilder(descriptionLength);
        for (int characterIndex = 0; characterIndex < descriptionLength; characterIndex++) {
            description.append(NAME_CHARACTERS.charAt(random.nextInt(NAME_CHARACTERS.length())));
        }

        List<String> transaction = new ArrayList<>(TRANSACTION_FIELDS);
        transaction.add(Integer.toString(transactionId));
        transaction.add(Integer.toString(customerId));
        // TransTotal: transaction total amount
        transaction.add(String.format(Locale.US, "%.2f", random.nextDouble(10.0, 1_000.0)));
        // TransNumItems: number of items in the transaction
        transaction.add(Integer.toString(random.nextInt(1, 11)));
        // TransDesc: transaction description - random string
        transaction.add(description.toString());
        return transaction;
    }

    public static void main(String[] args) {
        Datasets datasets = new Datasets();
        // Generate all customers and transactions datasets
        List<String> customers = datasets.createAllCustomers(MAX_CUSTOMERS);
        datasets.writeDataset(DatasetType.CUSTOMER, customers, CUSTOMER_FIELDS);
        List<String> transactions = datasets.createAllTransactions(MAX_TRANSACTIONS);
        datasets.writeDataset(DatasetType.TRANSACTION, transactions, TRANSACTION_FIELDS);
    }
}