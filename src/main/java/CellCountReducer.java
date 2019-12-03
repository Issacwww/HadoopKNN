import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;

public class CellCountReducer extends Reducer<Text, Point, Text, PointArrayWritable> {
    private static final Logger LOG = Logger.getLogger(CellCountReducer.class);

    private ArrayList<Point> points = new ArrayList<>();
    public void reduce(Text key, Iterable<Point> pointValues, Context context) throws IOException, InterruptedException {
       LOG.debug("Reducing...");
        for(Point point: pointValues){
            points.add(point);
        }
        context.write(key, new PointArrayWritable(points));
    }
}
