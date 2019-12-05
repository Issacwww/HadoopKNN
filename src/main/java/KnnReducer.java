import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

public class KnnReducer extends Reducer<Text, Text, Point, PointArrayWritable> {
    private int K;
    protected void setup(Context context) throws IOException,InterruptedException {
        Configuration conf = context.getConfiguration();
        K = conf.getInt("K", 3);
    }
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<Point> pointsInCell = new HashSet<>();
        for(Text value:values){
            String[] points = value.toString().split("#");
            for (String pointStr:points) {
                Point point = new Point(pointStr);
                System.out.println(point);
                pointsInCell.add(point);
            }
        }
        Knn knn = new Knn(pointsInCell, K);
        ArrayList<PointTuple> resInCell = knn.getKnnPoints();
        for(PointTuple res:resInCell){
            context.write(res.getOrigin(),new PointArrayWritable(res.getNeighbors()));
        }
    }
}
