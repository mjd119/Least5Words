// Modified from https://www.geeksforgeeks.org/how-to-find-top-n-records-using-mapreduce/ (Method 1)
// Derived from top n algorithm
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Least5WordsDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // Default changed from 10 to 5 by mjd119
        /* here we set our own custom parameter myValue with
         * default value 10. We will overwrite this value in CLI
         * at runtime.
         * Remember that both parameters are Strings and we
         * have convert them to numeric values when required.
         */

        conf.set("myValue", "5");

        String[] otherArgs = new GenericOptionsParser(conf,
                args).getRemainingArgs();

        // if less than two paths provided will show error
        if (otherArgs.length < 2) {
            System.err.println("Error: please provide two paths");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "bottom_5 program_2");
        job.setJarByClass(Least5WordsDriver.class);

        job.setMapperClass(Least5WordsMapper.class);
        job.setReducerClass(Least5WordsReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}