import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PartitionerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    private Text outputKey = new Text();
    private IntWritable outputValue = new IntWritable();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        String line = value.toString().trim();
        
        // Skip header
        if (line.startsWith("order_id")) {
            return;
        }
        
        String[] fields = line.split(",");
        
        if (fields.length >= 3) {
            String orderId = fields[0].trim();
            int quantity;
            
            try {
                quantity = Integer.parseInt(fields[2].trim());
            } catch (NumberFormatException e) {
                return;
            }
            
            outputKey.set(orderId);
            outputValue.set(quantity);
            
            context.write(outputKey, outputValue);
        }
    }
}