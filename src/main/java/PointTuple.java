import java.util.ArrayList;

public class PointTuple {

    private Point origin;
    private ArrayList<Point> neighbors;

    public PointTuple(Point origin, ArrayList<Point> neighbors) {
        this.origin = origin;
        this.neighbors = neighbors;
    }

    private double getEuclideanDistance(Point origin, Point neighbor){
        return Math.sqrt(Math.pow((origin.x - neighbor.x),2) + Math.pow((origin.y - neighbor.y),2));
    }

    public double getRadius(){
        return getEuclideanDistance(origin,neighbors.get(0));
    }

    @Override
    public String toString() {
        return "PointTuple{" +
                "origin=" + origin +
                ", neighbors=" + neighbors +
                '}';
    }


    public static void main(String[] args){
        Point o = new Point(0,0);
        Point a = new Point(1,2);
        Point b = new Point(3,4);

        ArrayList<Point> test = new ArrayList<>();
        test.add(a);
        test.add(b);

        System.out.println(new PointTuple(o,test));


    }
}
