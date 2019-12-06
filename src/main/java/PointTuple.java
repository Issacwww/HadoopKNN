import java.util.ArrayList;

public class PointTuple {

    private Point origin;
    private ArrayList<Point> neighbors;

    public PointTuple(Point origin, ArrayList<Point> neighbors) {
        this.origin = origin;
        this.neighbors = neighbors;
    }

    public double getRadius(){
        return origin.getEuclideanDistance(neighbors.get(0));
    }

    @Override
    public String toString() {
        return "PointTuple{" +
                "origin=" + origin +
                ", neighbors=" + neighbors +
                '}';
    }

    public Point getOrigin() {
        return origin;
    }

    public ArrayList<Point> getNeighbors() {
        return neighbors;
    }

    public static void main(String[] args){
        Point o = new Point("0",0,0);
        Point a = new Point("1",1,2);
        Point b = new Point("2",3,4);

        ArrayList<Point> test = new ArrayList<>();
        test.add(a);
        test.add(b);

        System.out.println(new PointTuple(o,test));


    }
}
