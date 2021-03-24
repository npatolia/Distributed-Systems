import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;


public class SalesCounter {

    public static void main(String[] args) {
        ArrayList<String> sales = fileToList("sales.txt");
        Map<String, Long> counts = sales
                .stream()
                .collect(Collectors.groupingBy(e -> e, Collectors.counting()));
        Map<String, Long> sortedMap = new TreeMap<>(counts);
        createResultFile(sortedMap);
    }

    public static void createResultFile(Map<String, Long> counts){
        String result = "";
        for (Map.Entry<String, Long> entry : counts.entrySet()) {
            result += entry.getKey().trim()+ " " +entry.getValue().toString().trim() + "\n";
        }

        result = result.trim();

        try {
            Files.write(Paths.get("result.txt"), result.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static ArrayList<String> fileToList(String filename) {
        ArrayList<String> list = new ArrayList<String>();
        try {
            Scanner s = new Scanner(new File(filename));
            while (s.hasNextLine()) {
                String[] tokens = s.nextLine().trim().split(",");
                list.add(tokens[1]);
            }
            s.close();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
        return list;
    }
}