import com.google.gson.Gson;

import java.util.HashMap;

public class QTree {
    int levels;
    double sideLen;
    Node root;
    public QTree(int levels, double sideLen){
        this.levels = levels + 1;
        this.sideLen = sideLen;
        this.root = new Node(0,0, sideLen, "0", "0");
        this.recursive_subdivide(this.root);
    }

    private void recursive_subdivide(Node node) {
        if (node.id.length() == this.levels) {
            node.isLeave = true;
            return;
        }
        /**
         *  divide the cell in following way at each call of this function
         *  1 3
         *  0 2
         */
        double subside = node.sideLen / 2;

        Node x0 = new Node(node.x, node.y, subside, node.id, node.id + "0");
        this.recursive_subdivide(x0);

        Node x1 = new Node(node.x, node.y + subside, subside, node.id, node.id + "1");
        this.recursive_subdivide(x1);

        Node x2 = new Node(node.x + subside, node.y, subside, node.id, node.id + "2");
        this.recursive_subdivide(x2);

        Node x3 = new Node(node.x + subside, node.y + subside, subside, node.id, node.id + "3");
        this.recursive_subdivide(x3);

        node.children.add(x0);
        node.children.add(x1);
        node.children.add(x2);
        node.children.add(x3);
    }

    public void display(Node node){
        if(node.isLeave){
            System.out.println(node);
            return;
        }
        System.out.println(node);
        for(Node child: node.children){
            display(child);
        }
    }

    public Node findNodeById(String id){
        if(id == "0")
            return this.root;
        Node cur = this.root;
        int curIdx = 1;
        while(curIdx < id.length()){
            cur = cur.children.get(Character.getNumericValue(id.charAt(curIdx)));
            curIdx++;
        }
        return cur;
    }

    public Node findNodeByCoords(Point point){
        return this.findNodeByCoords(this.root, point.x, point.y);
    }

    private Node findNodeByCoords(Node node, double x, double y) {
        if(node.isLeave) {
            node.points.add(new Point(x, y));
            return node;
        }
        double midX = node.x + node.sideLen / 2, midY =  node.y + node.sideLen / 2;
        if(x < midX && y < midY)
            return findNodeByCoords(node.children.get(0), x, y);
        else if(x < midX && y >= midY)
            return findNodeByCoords(node.children.get(1), x, y);
        else if(x >= midX && y < midY)
            return findNodeByCoords(node.children.get(2), x, y);
        return findNodeByCoords(node.children.get(3), x, y);
    }

    public void merge(Node node){
        for(Node child: node.children){
            if (child.points.isEmpty() && !child.isLeave)
                merge(child);
            node.points.addAll(child.points);
            child.isLeave = true;
        }
        node.isLeave = true;
        node.children.clear();
    }

    public static void main(String[] args) {
        QTree qt = new QTree(1,20);
        qt.findNodeByCoords(new Point(16.6,15));
        qt.findNodeByCoords(new Point(2,4));
        Gson gson = new Gson();

        System.out.println("Init tree");
        qt.display(qt.root);
//        System.out.println("Find parent of 02:"+qt.findNodeById(qt.root.children.get(2).getParentId()));

//        String gsonStr = gson.toJson(qt,QTree.class);
//        System.out.println("To Gson: "+gsonStr);
//        QTree qt2 = gson.fromJson(gsonStr,QTree.class);
//        System.out.println("Decode Gson");
//        qt2.display(qt2.root);
//
//        System.out.println("Merge Tree");
//        qt.merge(qt.root);
//        qt.display(qt.root);
//
//        gsonStr = gson.toJson(qt,QTree.class);
//        System.out.println("To Gson: "+gsonStr);
//        qt2 = gson.fromJson(gsonStr,QTree.class);
//        System.out.println("Decode Gson");
//        qt2.display(qt2.root);

    }

}
