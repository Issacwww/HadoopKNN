import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;

public class KnnMapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        HashSet<Point> pointList = new HashSet<>();
        String[] line = value.toString().split("\t");
        String[] points = line[1].split("#");
        for (String pointStr:points) {
            Point point = new Point(pointStr);
            pointList.add(point);
        }
        Text cellId = new Text();
        cellId.set(line[0]);
        PointArrayWritable out = new PointArrayWritable(pointList);
        context.write(cellId, new Text(out.toString()));
    }
}
