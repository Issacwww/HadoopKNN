import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class CellCountMapper2 extends Mapper<Object, Text, Text, IntWritable> {

    private static final Logger LOG = Logger.getLogger(CellCountMapper.class);
    private static IntWritable one = new IntWritable(1);
    private Text cellId = new Text();
    private QTree qTree;
    protected void setup(Context context) throws IOException,InterruptedException {
        Configuration conf = context.getConfiguration();
        int N = conf.getInt("N", 3);
        double S = conf.getDouble("S", 100);
        qTree = new QTree(N,S);
        LOG.debug("Mapper set up");
    }
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] pointInput = value.toString().split("\t");
        Point point = new Point(Double.parseDouble(pointInput[1]),Double.parseDouble(pointInput[2]));
        Node node = qTree.findNodeByCoords(point);
        cellId.set(node.id);
        context.write(cellId, one);
    }
}
