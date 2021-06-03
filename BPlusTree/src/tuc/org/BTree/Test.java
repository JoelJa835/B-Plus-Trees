package tuc.org.BTree;

public class Test {
    public static void main(String[] args) {
        // Demo with Data as data
        BTree<Integer, Data> tree = new BTree<Integer, Data>();

        tree.insert(Integer.valueOf(10), new Data(1,2,3,4));
        tree.insert(Integer.valueOf(20), new Data(5,6,7,8));
        tree.insert(Integer.valueOf(10), new Data(9,10,11,12));
        tree.insert(Integer.valueOf(10), new Data(13,14,15,16));
        tree.insert(Integer.valueOf(10), new Data(11,22,33,44));
        tree.insert(Integer.valueOf(10), new Data(111,222,333,444));

        System.out.println(tree.search(10));
        tree.delete(10);
        System.out.println(tree.search(20));
        tree.delete(10);

        System.out.println(tree.search(10));
        tree.delete(10);
        System.out.println(tree.search(10));
        tree.delete(10);
        System.out.println(tree.search(10));
        tree.delete(10);
        System.out.println(tree.search(10));
        tree.delete(10);

        System.out.println(tree.search(10));
        tree.delete(10);
        System.out.println(tree.search(10));
        tree.delete(10);
        System.out.println(tree.search(10));

    }
}
