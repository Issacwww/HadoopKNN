import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

public class KnnReducer extends Reducer<Text, Text, Point, Text> {
    private int K;
    protected void setup(Context context) throws IOException,InterruptedException {
        Configuration conf = context.getConfiguration();
        K = conf.getInt("K", 3);
    }
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<Point> pointsInCell = PointArrayWritable.parseTextIntoPoints(values);
        //run knn for these points
        Knn knn = new Knn(pointsInCell, K);
        ArrayList<PointTuple> resInCell = knn.getKnnPoints();
        for(PointTuple res:resInCell){
            context.write(res.getOrigin(),new Text(key.toString() + "#" + new PointArrayWritable(res.getNeighbors()).toString()));
        }
    }
}
