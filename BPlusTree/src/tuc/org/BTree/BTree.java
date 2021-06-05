package tuc.org.BTree;

import java.io.FileNotFoundException;
import java.io.IOException;

public class BTree<TKey extends Comparable<TKey>, TValue> {
    private BTreeNode<TKey> root;


    private int nextFreeDatafileByteOffset = 0; // for this assignment, we only create new, empty files. We keep here the next free byteoffset in our file

    public BTree() throws IOException {
        // this.root = new BTreeLeafNode<TKey, TValue>();
        // CHANGE FOR STORING ON FILE
        this.root = StorageCache.getInstance().newLeafNode();
        StorageCache.getInstance().flush();
    }

    /**
     * Insert a new key and its associated value into the B+ tree.
     */
    public void insert(TKey key, TValue value) throws IOException {
        this.root = StorageCache.getInstance().retrieveNode(this.root.getStorageDataPage());
        // CHANGE FOR STORING ON FILE
        nextFreeDatafileByteOffset = StorageCache.getInstance().newData((Data)value, nextFreeDatafileByteOffset);

        // CHANGE FOR STORING ON FILE
        BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
        leaf.insertKey(key, value);

        if (leaf.isOverflow()) {
            BTreeNode<TKey> n = leaf.dealOverflow();
            if (n != null)
                this.root = n;
        }

        // CHANGE FOR STORING ON FILE
        StorageCache.getInstance().flush();
    }

    /**
     * Search a key value on the tree and return its associated value.
     */
    public TValue search(TKey key) throws IOException {
        this.root = StorageCache.getInstance().retrieveNode(this.root.getStorageDataPage());
        BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);

        int index = leaf.search(key);
        return (index == -1) ? null : leaf.getValue(index);
    }

    /**
     * Delete a key and its associated value from the tree.
     */
    public void delete(TKey key) throws IOException {
        BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);

        if (leaf.delete(key) && leaf.isUnderflow()) {
            BTreeNode<TKey> n = leaf.dealUnderflow();
            if (n != null)
                this.root = n;
        }
        // CHANGE FOR STORING ON FILE
        StorageCache.getInstance().flush();
    }

    /**
     * Search the leaf node which should contain the specified key
     */
    @SuppressWarnings("unchecked")
    private BTreeLeafNode<TKey, TValue> findLeafNodeShouldContainKey(TKey key) throws IOException {
        BTreeNode<TKey> node = this.root;

        while (node.getNodeType() == TreeNodeType.InnerNode) {
            node = ((BTreeInnerNode<TKey>)node).getChild( node.search(key) );
        }

        return (BTreeLeafNode<TKey, TValue>)node;
    }
}