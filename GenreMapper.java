import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GenreMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    private Map<String, String> genreMap = new HashMap<>();
    private Text outputKey = new Text();
    private IntWritable outputValue = new IntWritable(1);
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            Path path = new Path(cacheFiles[0]);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length >= 2) {
                    genreMap.put(parts[0].trim(), parts[1].trim());
                }
            }
            reader.close();
        }
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        String line = value.toString().trim();
        
        // Skip header row
        if (line.startsWith("Poster_Link") || line.startsWith("Series_Title")) {
            return;
        }
        
        // Parse CSV (handle quoted fields)
        String[] fields = parseCSV(line);
        
        // Genre is at index 5 (sixth column)
        if (fields.length > 5) {
            String genreField = fields[5].trim();
            
            // Multiple genres are separated by commas, e.g., "Action, Adventure, Drama"
            String[] genres = genreField.split(",");
            
            for (String genre : genres) {
                String cleanGenre = genre.trim();
                
                // Check if genre exists in lookup map
                String genreName = genreMap.get(cleanGenre);
                if (genreName == null) {
                    // If not found in lookup, use the genre as is
                    genreName = cleanGenre;
                }
                
                outputKey.set(genreName);
                context.write(outputKey, outputValue);
            }
        }
    }
    
    // Simple CSV parser to handle quoted fields
    private String[] parseCSV(String line) {
        // Replace commas inside quotes temporarily
        StringBuilder result = new StringBuilder();
        boolean inQuotes = false;
        
        for (char c : line.toCharArray()) {
            if (c == '"') {
                inQuotes = !inQuotes;
                result.append(c);
            } else if (c == ',' && inQuotes) {
                result.append(';'); // Replace comma inside quotes
            } else {
                result.append(c);
            }
        }
        
        String processed = result.toString();
        String[] fields = processed.split(",");
        
        // Restore commas inside quotes
        for (int i = 0; i < fields.length; i++) {
            fields[i] = fields[i].replace(';', ',').replace("\"", "");
        }
        
        return fields;
    }
}