import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class KnnCrossCellReducer extends Reducer<Text, Text, Point, Text> {

    private int K;

    protected void setup(Context context) throws IOException,InterruptedException {
        Configuration conf = context.getConfiguration();
        K = conf.getInt("K", 3);
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text value: values) {
            String[] inputs = value.toString().split("#");
            Point origin = new Point(inputs[0]);
            boolean notCrossBoundary = new Boolean(inputs[K + 1]);
            if (notCrossBoundary) {
                String cellIdAndNeighbors = key.toString();
                for (int i = 1; i <= K; i++) {
                    cellIdAndNeighbors = cellIdAndNeighbors.concat("#" + inputs[i]);
                }
                context.write(origin, new Text(cellIdAndNeighbors));
            } else {
                HashSet<Point> pointsInCell = new HashSet<>();
                // original neighbors
                for (int i = 1; i <= K; i++) {
                    Point point = new Point(inputs[i]);
                    pointsInCell.add(point);
                }
                PointTuple result = PointTuple.getResult(origin, pointsInCell, K);
                context.write(result.getOrigin(), new Text(key.toString() + "#" + new PointArrayWritable(result.getNeighbors()).toString()));
            }
        }
    }



}
