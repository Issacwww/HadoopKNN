import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

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

    //TODO test required
    public HashSet<Point> toHashSet(){
        Point[] points = (Point[]) toArray();
        HashSet<Point> res = new HashSet<>();
        for (Point point: points) {
            res.add(point);
        }
        return res;
    }

    public String[] points() {
        return toStrings();
    }

    @Override
    public String toString() {
        return StringUtils.join(points(), "#");
    }

}
