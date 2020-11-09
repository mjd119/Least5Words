// Modified from https://www.geeksforgeeks.org/how-to-find-top-n-records-using-mapreduce/ (Method 1)
// Unchanged for top n algorithm
import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class Least5WordsMapper extends Mapper<Object,
        Text, LongWritable, Text> {

    // data format => movie_name
    // no_of_views (tab seperated)
    @Override
    public void map(Object key, Text value,
                    Context context) throws IOException,
            InterruptedException
    {

        String[] tokens = value.toString().split("\t");

        String movie_name = tokens[0];
        long no_of_views = Long.parseLong(tokens[1]);
        // Comment out so we don't have the highest values retrieved
        //no_of_views = (-1) * no_of_views;

        context.write(new LongWritable(no_of_views),
                new Text(movie_name));
    }
}

