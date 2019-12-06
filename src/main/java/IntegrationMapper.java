import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;

public class IntegrationMapper extends Mapper <Object, Text, Text, Text>{

    private int K;

    protected void setup(Context context) throws IOException,InterruptedException {
        Configuration conf = context.getConfiguration();
        K = conf.getInt("K", 3);
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println(value.toString());
        String[] inputs = value.toString().split("\t");
        Point originPoint = new Point(inputs[0]);
        String[] cellIdAndNeighbors = inputs[1].split("#");
        HashSet<Point> pointList = new HashSet<>();
        for (int i = 1; i < K; i++) {
            Point point = new Point(cellIdAndNeighbors[i]);
            pointList.add(point);
        }
        PointArrayWritable out = new PointArrayWritable(pointList);
        context.write(new Text(originPoint.toString()), new Text(out.toString()));
    }

}
