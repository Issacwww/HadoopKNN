import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;

public class IntegrationReducer extends Reducer<Text, Text, Point, PointArrayWritable> {
    private int K;

    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        K = conf.getInt("K", 3);
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<Point> pointsInCell = new HashSet<>();
        for (Text value : values) {
            String[] points = value.toString().split("#");
            for (String pointStr : points) {
                Point point = new Point(pointStr);
                pointsInCell.add(point);
            }
        }
        Point originPoint = new Point(key.toString());
        PointTuple result = getResult(originPoint, pointsInCell, K);
        context.write(originPoint, new PointArrayWritable(result.getNeighbors()));
    }

    private PointTuple getResult(Point origin, HashSet<Point> candidates, int k) {
        PriorityQueue<PointDetail> queue = new PriorityQueue<PointDetail>(k,
                new Comparator<PointDetail>() {
                    public int compare(PointDetail p1, PointDetail p2) {
                        if (p2.distance > p1.distance)
                            return 1;
                        return -1;
                    }
                });
        for (Point point : candidates) {
            PointDetail detail = new PointDetail(point, origin.getEuclideanDistance(point));
            if (queue.size() < k) {
                queue.offer(detail);
            } else if (queue.peek().distance > detail.distance) {
                queue.poll();
                queue.offer(detail);
            }
        }
        ArrayList<Point> neighbor = new ArrayList<>();
        while (!queue.isEmpty()){
            neighbor.add(queue.poll().point);
        }
        return new PointTuple(origin,neighbor);
    }
}
