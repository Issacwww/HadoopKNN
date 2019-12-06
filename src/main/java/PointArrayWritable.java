import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

import java.util.Collection;
import java.util.HashSet;

public class PointArrayWritable extends ArrayWritable {
    public PointArrayWritable(Collection<Point> pointsList) {
        super(PointArrayWritable.class);
        int length = pointsList.size();
        Point[] points = new Point[length];
        int i = 0;
        for (Point point: pointsList) {
            points[i] = new Point(point);
            i++;
        }
        set(points);
    }

    public static HashSet<Point> praseTextIntoPoints(Iterable<Text> values) {
        HashSet<Point> pointsInCell = new HashSet<>();
        for (Text value : values) {
            String[] points = value.toString().split("#");
            for (String pointStr : points) {
                Point point = new Point(pointStr);
                pointsInCell.add(point);
            }
        }
        return pointsInCell;
    }
    @Override
    public String toString() {
        return StringUtils.join(toStrings(), "#");
    }

}
