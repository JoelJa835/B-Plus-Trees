package tuc.org.BTree;

import java.io.IOException;

public class Test {
    public static void main(String[] args) throws IOException {
        // Demo with Data as data
        BTree tree = new BTree();

        for(int i=0;i<50;i++) {
            tree.insert(Integer.valueOf(i), new Data(1, 2, 3, 4, 5, 6, 7, i));
        }



        System.out.println(tree.search(10));
        tree.delete(10);
//        System.out.println(tree.search(20));
//        tree.delete(10);
//
//        System.out.println(tree.search(10));
//        tree.delete(10);
//        System.out.println(tree.search(10));
//        tree.delete(10);
//        System.out.println(tree.search(10));
//        tree.delete(10);
//        System.out.println(tree.search(10));
//        tree.delete(10);
//
//        System.out.println(tree.search(10));
//        tree.delete(10);
//        System.out.println(tree.search(10));
//        tree.delete(10);
//        System.out.println(tree.search(10));

    }
}
