import com.google.gson.Gson;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

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

        /****************     CONFIG      ********************/
        Configuration conf = new Configuration();
        String inputPath = args[0], outputPath = args[1];
        int K = Integer.parseInt(args[2]), N = Integer.parseInt(args[3]), R = Integer.parseInt(args[5]);
        double S = Double.parseDouble(args[4]);
        conf.setInt("K", K);
        conf.setInt("N", N);
        conf.setDouble("S", S);

        LOG.info("****************     STEP 1     ********************");
        Job job1 = Job.getInstance(conf, "Cell Counter");

        LOG.info("Job1 setting");
        job1.setJarByClass(HadoopKnn.class);
        job1.setMapperClass(CellCountMapper.class);
        job1.setReducerClass(CellCountReducer.class);
        //to get global view, set as one
        job1.setNumReduceTasks(1);
        //set input format
        job1.setInputFormatClass(TextInputFormat.class);
        //set map output key
        job1.setMapOutputKeyClass(Text.class);
        //set map output value
        job1.setMapOutputValueClass(Point.class);
        //set job1 output key
        job1.setOutputKeyClass(Text.class);
        //set job1 output value
        job1.setOutputValueClass(PointArrayWritable.class);
        //set input and output path
        FileInputFormat.setInputPaths(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(outputPath,"out1"));

        if (!job1.waitForCompletion(true)) {
            LOG.error("Running Job 1 Wrong");
            System.exit(1);
        }
        QTree qTree = new QTree(N, S);
        String mergedPoints ="data/mergedPoints";
        String mergedQTree = treeMerger(qTree, K, conf.get("fs.defaultFS"),
                new Path(outputPath ,"out1/part-r-00000"),
                new Path( mergedPoints));
        conf.set("qTree", mergedQTree);

        LOG.info("****************     STEP 2     ********************");
        Job job2 = Job.getInstance(conf,"Knn in cell");

        LOG.info("job2 setting");
        job2.setJarByClass(HadoopKnn.class);
        job2.setMapperClass(KnnMapper.class);
        job2.setReducerClass(KnnReducer.class);
        job2.setNumReduceTasks(R);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Point.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job2, new Path(mergedPoints));
        FileOutputFormat.setOutputPath(job2, new Path(outputPath,"out2"));
        if (!job2.waitForCompletion(true)) {
            LOG.error("Running Job 2 Wrong");
            System.exit(1);
        }

        LOG.info("****************     STEP 3     ********************");

        Job job3= Job.getInstance(conf,"Update Knn cross cells");

        LOG.info("job3 setting");
        job3.setJarByClass(HadoopKnn.class);
        job3.setMapperClass(KnnCrossCellMapper.class);
        job3.setReducerClass(KnnCrossCellReducer.class);
        job3.setNumReduceTasks(1);
        job3.setInputFormatClass(TextInputFormat.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Point.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(PointArrayWritable.class);
        FileInputFormat.setInputPaths(job3, new Path(inputPath));
        FileOutputFormat.setOutputPath(job3, new Path(outputPath));
        FileOutputFormat.setOutputPath(job3, new Path(outputPath,"out3"));
//        if (!job3.waitForCompletion(true)) {
//            LOG.error("Running Job 3 Wrong");
//            System.exit(1);
//        }

        LOG.info("****************     STEP 4     ********************");

//        Job job4 = Job.getInstance(conf,"Integration");
//        LOG.info("job4 setting");
//        job4.setJarByClass(HadoopKnn.class);
//        job4.setMapperClass(CellCountMapper.class);
//        job4.setReducerClass(CellCountReducer.class);
//        job4.setNumReduceTasks(1);
//        job4.setInputFormatClass(TextInputFormat.class);
//        job4.setMapOutputKeyClass(Text.class);
//        job4.setMapOutputValueClass(Point.class);
//        job4.setOutputKeyClass(Text.class);
//        job4.setOutputValueClass(PointArrayWritable.class);
//        FileInputFormat.setInputPaths(job4, new Path(inputPath));
//        FileOutputFormat.setOutputPath(job4, new Path(outputPath));
//        FileOutputFormat.setOutputPath(job4, new Path(outputPath,"out4"));
//        if (!job4.waitForCompletion(true)) {
//            LOG.error("Running Job 4 Wrong");
//            System.exit(1);
//        }

        LOG.info("All jobs succeed!");
        System.exit(0);


    }
    public static String treeMerger(QTree qTree, int K, String hdfsuri, Path stepOneOut, Path mergedPoints) throws IOException {
        Configuration conf = new Configuration();
        Gson gson = new Gson();
//        // Set FileSystem URI
        conf.set("fs.defaultFS", hdfsuri);
//        // Because of Maven
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
//        // Set HADOOP user
//        System.setProperty("HADOOP_USER_NAME", "hdfs");
//        System.setProperty("hadoop.home.dir", "/");
        //Get the filesystem - HDFS
        FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);
        //Init input stream
        FSDataInputStream inputStream = fs.open(stepOneOut);
        //Classical input stream usage
        String out= IOUtils.toString(inputStream, "UTF-8");
        String[] lines = out.split("\n");
        Queue<String> toMerge = new LinkedList<>();
        for(String line : lines){
            String[] tmp = line.split("\t");
            Node n = qTree.findNodeById(tmp[0]);
            n.parsePointsFromString(tmp[1]);
            if(n.getSum() <= K){
                toMerge.add(n.id);
            }
        }
//        LOG.info("Step1 Before Merge");
//        qTree.display();
        while(!toMerge.isEmpty()) {
            String id = toMerge.poll();
            Node n = qTree.findNodeById(id);
            if (n != null) {
                qTree.merge(qTree.findNodeById(n.getParentId()));
                Node parent = qTree.findNodeById(n.getParentId());
                if (parent.getSum() <= K){
                    toMerge.add(parent.id);
                }
            }
        }

        HashMap<String, String> leavesPoints = qTree.getLeavesPoints(true);
        //TODO: write the data to HDFS
        //Init output stream
        FSDataOutputStream outputStream=fs.create(mergedPoints);
        for(Map.Entry<String, String> entry: leavesPoints.entrySet()){
            //Cassical output stream usage
            outputStream.writeBytes(entry.getKey()+"\t"+entry.getValue());
        }
        outputStream.close();
        LOG.info("End Write file into hdfs");
        LOG.info("Merged, transfer to gson");
        //Only save the structure
        String gsonStr = gson.toJson(qTree,QTree.class);
        return gsonStr;
    }
}
