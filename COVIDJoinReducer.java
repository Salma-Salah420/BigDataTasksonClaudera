import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class COVIDJoinReducer extends Reducer<Text, Text, Text, Text> {
    
    private Text outputValue = new Text();
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        
        List<String> casesData = new ArrayList<>();
        List<String> populationData = new ArrayList<>();
        
        // Separate data by source
        for (Text val : values) {
            String line = val.toString();
            String source = line.substring(0, 1);
            String data = line.substring(2);
            
            if (source.equals("C")) {
                casesData.add(data);
            } else if (source.equals("R")) {
                populationData.add(data);
            }
        }
        
        // Calculate total cases from all records for this country
        int totalCases = 0;
        for (String cases : casesData) {
            // Extract number from "cases=123"
            String[] parts = cases.split("=");
            if (parts.length >= 2) {
                try {
                    totalCases += Integer.parseInt(parts[1]);
                } catch (NumberFormatException e) {}
            }
        }
        
        // Get population (should be one record per country)
        String population = "N/A";
        if (!populationData.isEmpty()) {
            String[] parts = populationData.get(0).split("=");
            if (parts.length >= 2) {
                population = parts[1];
            }
        }
        
        // Case 1: Has cases AND population
        if (totalCases > 0 && !population.equals("N/A")) {
            // Calculate cases per million
            double pop = Double.parseDouble(population);
            double casesPerMillion = (totalCases / pop) * 1000000;
            
            String result = String.format("Population: %s | Total Cases: %d | Cases per Million: %.2f",
                                          population, totalCases, casesPerMillion);
            outputValue.set(result);
            context.write(key, outputValue);
        }
        // Case 2: Has cases but NO population
        else if (totalCases > 0 && population.equals("N/A")) {
            outputValue.set("[MISSING POPULATION DATA] -> Total Cases: " + totalCases);
            context.write(key, outputValue);
        }
        // Case 3: Has population but NO cases
        else if (totalCases == 0 && !population.equals("N/A")) {
            outputValue.set("Population: " + population + " -> [NO COVID CASES REPORTED]");
            context.write(key, outputValue);
        }
    }
}