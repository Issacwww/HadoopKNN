import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;

public class PointArrayWritable extends ArrayWritable {
    public PointArrayWritable(){
        super(PointArrayWritable.class);
    }
    public PointArrayWritable(ArrayList<Point> pointsList) {
        super(PointArrayWritable.class);
        int length = pointsList.size();

        ObjectWritable[] points = new ObjectWritable[length];
        for (int i = 0; i < length; i++) {
            points[i] = new ObjectWritable(pointsList.get(i));
        }
        set(points);
    }


    public String[] points() {
        return toStrings();
    }
    @Override
    public String toString() {
        return StringUtils.join(points(), ",");
    }
}
