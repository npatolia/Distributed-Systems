import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;


public class TopProducts {

    public static void main(String[] args) {
        Map<Double, String> sales = fileToMap("product.txt");
        LinkedHashMap<Double, String> sortedMap = new LinkedHashMap<>();
        sales.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey(Comparator.reverseOrder()))
                .forEachOrdered(x -> sortedMap.put(x.getKey(), x.getValue()));
        createResultFile(sortedMap);
    }

    public static void createResultFile(Map<Double, String> sales){
        String result = "";
        int counter = 0;
        for (Map.Entry<Double, String> entry : sales.entrySet()) {
            result += entry.getValue() + ", " + entry.getKey() + "\n";
            if(counter >= 9){
                break;
            }
            counter ++;
        }

        result = result.trim();

        try {
            Files.write(Paths.get("result.txt"), result.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Map<Double, String> fileToMap(String filename) {
        Map<Double, String> map = new HashMap<>();
        try {
            Scanner s = new Scanner(new File(filename));
            while (s.hasNextLine()) {
                String[] tokens = s.nextLine().trim().split(",");
                map.put(Double.parseDouble(tokens[2]), tokens[0].trim() + ", " + tokens[1].trim());
            }
            s.close();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
        return map;
    }
}