import java.io.File; // Import the File class
import java.io.FileNotFoundException; // Import this class to handle errors
import java.util.Scanner; // Import the Scanner class to read text files
import java.util.ArrayList;
import java.util.Random;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.IOException;

public class Lab1 {
    public static void main(String[] args) {
        ArrayList<String> businessNames = fileToList("lists/business-names.txt", 1);
        ArrayList<String> cityDetails = fileToList("lists/cities.txt", 2);
        ArrayList<String> firstNames = fileToList("lists/common-first.txt", 1);
        ArrayList<String> lastNames = fileToList("lists/common-last.txt", 1);
        ArrayList<String> phoneNumbers = fileToList("lists/phone-numbers.txt", 1);
        ArrayList<String> streetAddrs = fileToList("lists/street-addresses.txt", 1);
        ArrayList<String> products = fileToList("lists/Flipkart-products.txt", 1);
        int numStores = 100;
        int numCustomers = 1000;
        int numSales = 2000;
        int numProducts = 100;
        int numLineItems = 4000;

        createStoreFile(numStores, businessNames, streetAddrs, cityDetails, phoneNumbers);
        createCustomerFile(numCustomers, firstNames, lastNames, streetAddrs, cityDetails, phoneNumbers);
        createSalesFile(numSales, numStores, numCustomers);
        createProductFile(numProducts, products);
        createLineItemFile(numLineItems, numSales, numProducts);

    }

    public static void createStoreFile(int numEntries, ArrayList<String> businessNames, ArrayList<String> streetAddrs,
            ArrayList<String> cityDetails, ArrayList<String> phoneNumbers) {
        String entry = "";

        for (int i = 1; i < numEntries + 1; i++) {
            entry += String.valueOf(i) + ", ";
            entry += businessNames.get(new Random().nextInt(businessNames.size())) + ", ";
            entry += streetAddrs.get(new Random().nextInt(streetAddrs.size())) + ", ";
            entry += cityDetails.get(new Random().nextInt(cityDetails.size())) + ", ";
            entry += phoneNumbers.get(new Random().nextInt(phoneNumbers.size())) + "\n";
        }

        entry = entry.trim();

        try {
            Files.write(Paths.get("result/store.txt"), entry.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createCustomerFile(int numEntries, ArrayList<String> firstNames, ArrayList<String> lastNames,
            ArrayList<String> streetAddrs, ArrayList<String> cityDetails, ArrayList<String> phoneNumbers) {
        String entry = "";

        for (int i = 1; i < numEntries + 1; i++) {
            entry += String.valueOf(i) + ", ";
            entry += firstNames.get(new Random().nextInt(firstNames.size())) + " ";
            entry += lastNames.get(new Random().nextInt(lastNames.size())) + ", ";
            entry += createDate(1950, 60) + ", ";
            entry += streetAddrs.get(new Random().nextInt(streetAddrs.size())) + ", ";
            entry += cityDetails.get(new Random().nextInt(cityDetails.size())) + ", ";
            entry += phoneNumbers.get(new Random().nextInt(phoneNumbers.size())) + "\n";
        }

        entry = entry.trim();

        try {
            Files.write(Paths.get("result/customer.txt"), entry.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createSalesFile(int numEntries, int numStores, int numCustomers) {
        String entry = "";

        for (int i = 1; i < numEntries + 1; i++) {
            entry += String.valueOf(i) + ", ";
            entry += createDate(2010, 10) + ", ";
            entry += createTime() + ", ";
            entry += String.valueOf(new Random().nextInt(numStores) + 1) + ", ";
            entry += String.valueOf(new Random().nextInt(numCustomers) + 1) + "\n";
        }

        entry = entry.trim();

        try {
            Files.write(Paths.get("result/sales.txt"), entry.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createProductFile(int numEntries, ArrayList<String> products) {
        String entry = "";

        for (int i = 1; i < numEntries + 1; i++) {
            entry += String.valueOf(i) + ", ";
            entry += products.get(new Random().nextInt(products.size())) + ", ";
            entry += String.valueOf(String.format("%.2f", new Random().nextInt(50000) / 100.0)) + "\n";
        }

        entry = entry.trim();

        try {
            Files.write(Paths.get("result/product.txt"), entry.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createLineItemFile(int numEntries, int numSales, int numProducts) {
        String entry = "";

        for (int i = 1; i < numEntries + 1; i++) {
            entry += String.valueOf(i) + ", ";
            entry += String.valueOf(new Random().nextInt(numSales) + 1) + ", ";
            entry += String.valueOf(new Random().nextInt(numProducts) + 1) + ", ";
            entry += String.valueOf(new Random().nextInt(15) + 1) + "\n";
        }

        entry = entry.trim();

        try {
            Files.write(Paths.get("result/lineItem.txt"), entry.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static ArrayList<String> fileToList(String filename, int flag) {
        ArrayList<String> list = new ArrayList<String>();
        try {
            Scanner s = new Scanner(new File(filename));
            while (s.hasNextLine()) {
                if(flag == 1){
                    list.add(s.nextLine().trim().replace(",", ""));
                } else {
                    list.add(s.nextLine().trim());
                }
            }
            s.close();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
        return list;
    }

    public static String createDate(int yearStart, int range) {
        String birthdayString = "";
        birthdayString += String.valueOf(new Random().nextInt(range) + yearStart) + '/';
        birthdayString += String.valueOf(new Random().nextInt(12) + 1) + '/';
        birthdayString += String.valueOf(new Random().nextInt(28) + 1);
        return birthdayString;
    }

    public static String createTime() {
        String time = "";
        time += String.valueOf(String.format("%02d", new Random().nextInt(24))) + ":";
        time += String.valueOf(String.format("%02d", new Random().nextInt(60))) + ":";
        time += String.valueOf(String.format("%02d", new Random().nextInt(60)));

        return time;
    }

}