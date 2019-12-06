import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;

public class PointTuple {

    private Point origin;
    private ArrayList<Point> neighbors;

    public PointTuple(Point origin, ArrayList<Point> neighbors) {
        this.origin = origin;
        this.neighbors = neighbors;
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

    public static PointTuple getResult(Point origin, HashSet<Point> candidates, int k) {
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
