import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.ArrayWritable;

import java.util.Collection;

public class PointArrayWritable extends ArrayWritable {
    public PointArrayWritable(){
        super(PointArrayWritable.class);
    }
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

    @Override
    public String toString() {
        return StringUtils.join(toStrings(), "#");
    }

}
