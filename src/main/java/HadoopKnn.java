import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HadoopKnn {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "hadoopknn");
        job.setJarByClass(HadoopKnn.class);
//        job.setMapperClass(WikiMapper.class);
//        job.setReducerClass(WikiReducer.class);
//        job.setCombinerClass(WikiReducer.class);

        //(node#)16 * (container#)16 * 1.75 = 448
        job.setNumReduceTasks(448);

        //set input format
        job.setInputFormatClass(TextInputFormat.class);
        //set output format
//        job.setOutputFormatClass(MyOutputFormat.class);

        //set map output key
        job.setMapOutputKeyClass(Text.class);
        //set map output value
//        job.setMapOutputValueClass(IntArrayWritable.class);
        //set job output key
        job.setOutputKeyClass(Text.class);
        //set job output value
//        job.setOutputValueClass(IntArrayWritable.class);

        //set input and output path
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
