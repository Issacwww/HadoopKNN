import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class CellCountReducer extends Reducer<Text, Point, Text, PointArrayWritable> {
    // Compute the data distribution in each cell using MapReduce
    public void reduce(Text key, Iterable<Point> pointValues, Context context) throws IOException, InterruptedException {
        HashSet<Point> points = new HashSet<>();
        //reduce the points list by the cellId
        for(Point point: pointValues){
            points.add(new Point(point));
        }
        context.write(key, new PointArrayWritable(points));
    }
}
