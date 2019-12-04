import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;

public class CellCountReducer extends Reducer<Text, Point, Text, PointArrayWritable> {
    private static final Logger LOG = Logger.getLogger(CellCountReducer.class);
//    private int K;
//    private QTree qTree;
//    protected void setup(Context context) throws IOException,InterruptedException {
//        Configuration conf = context.getConfiguration();
//        int N = conf.getInt("N", 3);
//        double S = conf.getDouble("S", 100);
//        K = conf.getInt("K", 5);
//        qTree = new QTree(N,S);
//    }
    public void reduce(Text key, Iterable<Point> pointValues, Context context) throws IOException, InterruptedException {
        ArrayList<Point> points = new ArrayList<>();
        LOG.debug("Reducing...");
        for(Point point: pointValues){
            points.add(point);
        }
        context.write(key, new PointArrayWritable(points));
    }
}
