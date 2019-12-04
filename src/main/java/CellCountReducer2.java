import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;

public class CellCountReducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {
//    private static final Logger LOG = Logger.getLogger(CellCountReducer.class);
//
//    private ArrayList<Point> points = new ArrayList<>();
//    private
//    private int K;
//    protected void setup(Context context) throws IOException,InterruptedException {
//        Configuration conf = context.getConfiguration();
//        K = conf.getInt("K", 5);
//
//    }
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable v: values){
            sum += v.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
