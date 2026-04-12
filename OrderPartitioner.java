import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartitioner extends Partitioner<Text, IntWritable> {
    
    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions) {
        // Partition based on order_id hash code
        String orderId = key.toString();
        
        // Extract order number from ID (e.g., ORD001 -> 1)
        String numPart = orderId.replaceAll("[^0-9]", "");
        int orderNum = numPart.isEmpty() ? 0 : Integer.parseInt(numPart);
        
        return Math.abs(orderNum % numPartitions);
    }
}