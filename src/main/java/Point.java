import org.apache.hadoop.io.ObjectWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class Point extends ObjectWritable {
    String id;
    double x;
    double y;
    public Point(){}
    public Point(Point other){
        this.id = other.id;
        this.x = other.x;
        this.y = other.y;
    }
    public Point(String id,double x, double y){
        this.id = id;
        this.x = x;
        this.y = y;
    }
    public Point(String pointStr){
        String[] pointInfo = pointStr.substring(1,pointStr.length()-1).split(",");
        this.id = pointInfo[0];
        this.x = Double.parseDouble(pointInfo[1]);
        this.y = Double.parseDouble(pointInfo[2]);
    }
    @Override
    public String toString() {
        return "("+id+","+x+","+y+")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Point point = (Point) o;
        return Double.compare(point.x, x) == 0 &&
                Double.compare(point.y, y) == 0 &&
                id.equals(point.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.toString());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeDouble(x);
        dataOutput.writeDouble(y);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readUTF();
        x = dataInput.readDouble();
        y = dataInput.readDouble();

    }

    public static void main(String[] args) {
        Point p = new Point("(4,1.349,5.123)");
        System.out.println(p);
    }
}
