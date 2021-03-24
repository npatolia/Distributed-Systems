import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;


public class Sales {

    public static void main(String[] args) {
        Map<String, ArrayList<String>> sales = fileToList("sales.txt");
        Map<String, ArrayList<String>> sortedMap = new TreeMap<>(sales);
        createResultFile(sortedMap);
    }

    public static void createResultFile(Map<String, ArrayList<String>> sales){
        String result = "";
        for (Map.Entry<String, ArrayList<String>> entry : sales.entrySet()) {
            Collections.sort(entry.getValue());
            result += entry.getKey()+ " " + String.join(", ", entry.getValue()) + "\n";
        }

        result = result.trim();

        try {
            Files.write(Paths.get("result.txt"), result.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Map<String, ArrayList<String>> fileToList(String filename) {
        Map<String, ArrayList<String >> map = new HashMap<>();
        try {
            Scanner s = new Scanner(new File(filename));
            while (s.hasNextLine()) {
                String[] tokens = s.nextLine().trim().split(",");
                if (map.containsKey(tokens[1].trim())){
                    map.get(tokens[1].trim()).add(tokens[2].trim());
                } else {
                    ArrayList<String> timesList = new ArrayList<String>();
                    timesList.add(tokens[2].trim());
                    map.put(tokens[1].trim(), timesList);
                }
            }
            s.close();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
        return map;
    }
}