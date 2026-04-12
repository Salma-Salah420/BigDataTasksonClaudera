import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RegionDetailsMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private Text outputKey = new Text();
    private Text outputValue = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        String line = value.toString().trim();
        
        // Skip header row
        if (line.startsWith("Country") || line.startsWith("country")) {
            return;
        }
        
        if (line.isEmpty()) {
            return;
        }
        
        String[] fields = line.split(",");
        
        if (fields.length >= 2) {
            String country = fields[0].trim();
            String population = fields[1].trim();
            
            // التأكد من صحة الرقم
            try {
                Long.parseLong(population);
            } catch (NumberFormatException e) {
                return; // نتخطى لو الرقم مش صحيح
            }
            
            outputKey.set(country);
            outputValue.set("R|population=" + population);
            
            context.write(outputKey, outputValue);
        }
    }
}