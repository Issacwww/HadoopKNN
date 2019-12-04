import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.log4j.Logger;

public class HadoopKnn {

    private static final Logger LOG = Logger.getLogger(HadoopKnn.class);
    /**
     *
     * @param args: <inputPath>, <outputPath>, <K neighbors>, <N levels>, <S sideLen> <R reduceTasks>
     */
    public static void main(String[] args) throws Exception {
        if(args.length < 6){
            System.out.println("Please input valid arguments: <inputPath>, <outputPath>, <K neighbors>, " +
                    "<N levels>, <S sideLen> <R reduceTasks>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        String inputPath = args[0], outputPath = args[1];
        int K = Integer.parseInt(args[2]), N = Integer.parseInt(args[3]), R = Integer.parseInt(args[5]);
        double S = Double.parseDouble(args[4]);
        conf.setInt("K", K);
        conf.setInt("N", N);
        conf.setDouble("S", S);
        LOG.debug("N:"+N+"R:"+R);
        Job job = Job.getInstance(conf, "hadoopknn");

        LOG.debug("Set Mapper and Reducer");
        job.setJarByClass(HadoopKnn.class);
        job.setMapperClass(CellCountMapper.class);
        job.setReducerClass(CellCountReducer.class);
//        job.setCombinerClass(WikiReducer.class);

        //(node#)16 * (container#)16 * 1.75 = 448
        job.setNumReduceTasks(R);

        LOG.debug("Set Format");

        //set input format
        job.setInputFormatClass(TextInputFormat.class);
        //set output format
//        job.setOutputFormatClass(MyOutputFormat.class);

        //set map output key
        job.setMapOutputKeyClass(Text.class);
        //set map output value
        job.setMapOutputValueClass(Point.class);
        //set job output key
        job.setOutputKeyClass(Text.class);
        //set job output value
        job.setOutputValueClass(PointArrayWritable.class);

        //set input and output path
        LOG.debug("Set Path");
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        LOG.debug("Run Job...");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
