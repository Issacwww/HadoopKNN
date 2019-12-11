import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class IntegrationReducer extends Reducer<Text, Text, Point, PointArrayWritable> {
    private int K;

    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        K = conf.getInt("K", 3);
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<Point> pointsInCell = PointArrayWritable.parseTextIntoPoints(values);
        Point originPoint = new Point(key.toString());
        PointTuple result = PointTuple.getResult(originPoint, pointsInCell, K);
        //reduce the final result
        context.write(originPoint, new PointArrayWritable(result.getNeighbors()));
    }




}
