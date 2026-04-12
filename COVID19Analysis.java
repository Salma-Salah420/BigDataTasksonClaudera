import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class COVID19Analysis {
    
    public static void main(String[] args) throws Exception {
        
        if (args.length != 3) {
            System.err.println("Usage: COVID19Analysis <cases_file_path> <region_details_file_path> <output_path>");
            System.err.println("Example: COVID19Analysis /input/covid_cases.csv /input/regions.csv /output/covid_analysis");
            System.exit(-1);
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "COVID-19 Analysis - Join Cases with Region Details");
        
        job.setJarByClass(COVID19Analysis.class);
        
        job.setReducerClass(COVIDJoinReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // Set the two input paths with their specific mappers
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, COVIDCasesMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RegionDetailsMapper.class);
        
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}