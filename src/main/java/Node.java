import java.io.IOException;
import java.util.ArrayList;

public class Node {
    double x;
    double y;
    double sideLen;
    String id;
    boolean isLeave;
    private String parentId;
    ArrayList<Point> points;
    ArrayList<Node> children;
    public Node(){}
    public Node(double x0, double y0, double sideLen, String parent, String id){
        this.x = x0;
        this.y = y0;
        this.sideLen = sideLen;
        this.parentId = parent;
        this.id = id;
        this.points = new ArrayList<>();
        this.children = new ArrayList<>();
        this.isLeave = false;
    }

    public void parsePointsFromString(String pointsString) throws IOException {
        String[] pointStr = pointsString.split("#");
        for(String point:pointStr)
            this.points.add(new Point(point));

    }
    public String getParentId() {
        return parentId;
    }

    public int getSum() {
        return this.points.size();
    }

    @Override
    public String toString() {
        StringBuilder childrenStr = new StringBuilder("["),pointsStr = new StringBuilder("[");
        if(!children.isEmpty()) {
            for (Node child : children)
                childrenStr.append("'" + child.id + "',");
            childrenStr.replace(childrenStr.length() - 1, childrenStr.length(), "]");
        }else
            childrenStr.append("]");
        if(!points.isEmpty()) {
            for (Point point : points)
                pointsStr.append(point + "#");
            pointsStr.replace(pointsStr.length() - 1, pointsStr.length(), "]");
        }else
            pointsStr.append("]");
        return "Node {" +
                "x=" + x +
                ", y=" + y +
                ", sideLen=" + sideLen +
                ", id='" + id + '\'' +
                ", parent='" + parentId + '\'' +
                ", children=" + childrenStr.toString() +
                ", points=" + pointsStr.toString() +
                "}";
    }
}
