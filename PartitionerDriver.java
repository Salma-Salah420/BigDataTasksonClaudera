import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PartitionerDriver {
    
    public static void main(String[] args) throws Exception {
        
        if (args.length != 2) {
            System.err.println("Usage: PartitionerDriver <input_path> <output_path>");
            System.err.println("Example: PartitionerDriver /input/orders.csv /output/order_quantity");
            System.exit(-1);
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Order Quantity Aggregation with Partitioner");
        
        job.setJarByClass(PartitionerDriver.class);
        
        job.setMapperClass(PartitionerMapper.class);
        job.setReducerClass(PartitionerReducer.class);
        job.setPartitionerClass(OrderPartitioner.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // Set number of reducers (partitions)
        job.setNumReduceTasks(3);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}