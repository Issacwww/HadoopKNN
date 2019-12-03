import java.util.ArrayList;

public class Node {
    double x;
    double y;
    double sideLen;
    String id;
    private Node parent;
    ArrayList<Point> points;
    ArrayList<Node> children;
    public Node(){}
    public Node(double x0, double y0, double sideLen, Node parent, String id){
        this.x = x0;
        this.y = y0;
        this.sideLen = sideLen;
        this.parent = parent;
        this.id = id;
        this.points = new ArrayList<>();
        this.children = new ArrayList<>();
    }

    public Node getParent() {
        return parent;
    }

    public int getSum() {
        return this.points.size();
    }

    @Override
    public String toString() {
        StringBuilder childrenStr = new StringBuilder("[ ");
        for(Node child :children)
            childrenStr.append("'" + child.id + "', ");
        childrenStr.append("]");
        StringBuilder pointsStr = new StringBuilder("[");
        for(Point point :points)
            pointsStr.append(point);
        pointsStr.append("]");
        return "Node {" +
                "x=" + x +
                ", y=" + y +
                ", sideLen=" + sideLen +
                ", id='" + id + '\'' +
                ", parent='" + parent.id + '\'' +
                ", children=" + childrenStr.toString() +
                ", points=" + pointsStr.toString() +
                "}";
    }
}
