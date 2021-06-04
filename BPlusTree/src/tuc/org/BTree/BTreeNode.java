package tuc.org.BTree;

import java.io.IOException;

enum TreeNodeType {
    InnerNode,
    LeafNode
}

abstract class BTreeNode<TKey extends Comparable<TKey>> {
    protected Integer[] keys;
    protected int keyCount;

    protected Integer parentNode;
    protected Integer leftSibling;
    protected Integer rightSibling;

    private boolean dirty;
    private int storageDataPage; // this node is stored at data page storageDataPage in the node/index file


    protected BTreeNode() {
        this.keyCount = 0;
        this.parentNode = null;
        this.leftSibling = null;
        this.rightSibling = null;

        this.dirty = false;
        this.storageDataPage = -1;
    }

    public void setStorageDataPage(int storageDataPage) {
        this.storageDataPage = storageDataPage;
    }
    public int getStorageDataPage() {
        return this.storageDataPage;
    }

    public boolean isDirty() {
        return this.dirty;
    }
    public void setDirty() {
        this.dirty = true;
    }

    public int getKeyCount() {
        return this.keyCount;
    }

    @SuppressWarnings("unchecked")
    public TKey getKey(int index) {
        return (TKey)this.keys[index];
    }

    public void setKey(int index, TKey key) {
        setDirty(); // we changed a key, so this node is dirty and must be flushed to disk
        this.keys[index] = (Integer) key;
    }

    public BTreeNode<TKey> getParent() {
        //return this.parentNode;
        // CHANGE FOR STORING ON FILE
        return StorageCache.getInstance().retrieveNode(this.parentNode);
    }

    public void setParent(BTreeNode<TKey> parent) {
        setDirty(); // we changed the parent, so this node is dirty and must be flushed to disk
        //this.parentNode = parent;

        // CHANGE FOR STORING ON FILE
        this.parentNode = parent.getStorageDataPage();
    }

    public abstract TreeNodeType getNodeType();


    /**
     * Search a key on current node, if found the key then return its position,
     * otherwise return -1 for a leaf node,
     * return the child node index which should contain the key for a internal node.
     */
    public abstract int search(TKey key);



    /* The codes below are used to support insertion operation */

    public boolean isOverflow() {
        return this.getKeyCount() == this.keys.length;
    }

    public BTreeNode<TKey> dealOverflow() {

        BTreeNode<TKey> rightSibling =  (BTreeNode<TKey>)StorageCache.getInstance().retrieveNode(this.rightSibling);
        int midIndex = this.getKeyCount() / 2;
        TKey upKey = this.getKey(midIndex);

        BTreeNode<TKey> newRNode = this.split();

        if (this.getParent() == null) {
            this.setParent(new BTreeInnerNode<TKey>());
        }
        newRNode.setParent(this.getParent());

        // maintain links of sibling nodes
        newRNode.setLeftSibling(this);
        newRNode.setRightSibling(rightSibling);
        if (this.getRightSibling() != null)
            this.getRightSibling().setLeftSibling(newRNode);
        this.setRightSibling(newRNode);

        // push up a key to parent internal node
        return this.getParent().pushUpKey(upKey, this, newRNode);
    }

    protected abstract BTreeNode<TKey> split();

    protected abstract BTreeNode<TKey> pushUpKey(TKey key, BTreeNode<TKey> leftChild, BTreeNode<TKey> rightNode);






    /* The codes below are used to support deletion operation */

    public boolean isUnderflow() {
        return this.getKeyCount() < (this.keys.length / 2);
    }

    public boolean canLendAKey() {
        return this.getKeyCount() > (this.keys.length / 2);
    }

    public BTreeNode<TKey> getLeftSibling() {
        BTreeNode<TKey> leftSibling =  (BTreeNode<TKey>)StorageCache.getInstance().retrieveNode(this.leftSibling);
        if (leftSibling != null && leftSibling.getParent() == this.getParent())
            return leftSibling;
        return null;
    }

    public void setLeftSibling(BTreeNode<TKey> sibling) {
        this.leftSibling = sibling.getStorageDataPage();
        setDirty(); // we changed a sibling, so this node is dirty and must be flushed to disk
    }

    public BTreeNode<TKey> getRightSibling() {
        BTreeNode<TKey> rightSibling =  (BTreeNode<TKey>)StorageCache.getInstance().retrieveNode(this.rightSibling);
        if (rightSibling != null && rightSibling.getParent() == this.getParent())
            return rightSibling;
        return null;
    }

    public void setRightSibling(BTreeNode<TKey> sibling) {
        //this.rightSibling = silbling;
        // CHANGE FOR STORING ON FILE
        this.rightSibling = sibling.getStorageDataPage();
        setDirty(); // we changed a sibling, so this node is dirty and must be flushed to disk

    }

    public BTreeNode<TKey> dealUnderflow() {
        if (this.getParent() == null)
            return null;

        // try to borrow a key from sibling
        BTreeNode<TKey> leftSibling = this.getLeftSibling();
        if (leftSibling != null && leftSibling.canLendAKey()) {
            this.getParent().processChildrenTransfer(this, leftSibling, leftSibling.getKeyCount() - 1);
            return null;
        }

        BTreeNode<TKey> rightSibling = this.getRightSibling();
        if (rightSibling != null && rightSibling.canLendAKey()) {
            this.getParent().processChildrenTransfer(this, rightSibling, 0);
            return null;
        }

        // Can not borrow a key from any sibling, then do fusion with sibling
        if (leftSibling != null) {
            return this.getParent().processChildrenFusion(leftSibling, this);
        }
        else {
            return this.getParent().processChildrenFusion(this, rightSibling);
        }
    }

    protected abstract void processChildrenTransfer(BTreeNode<TKey> borrower, BTreeNode<TKey> lender, int borrowIndex);

    protected abstract BTreeNode<TKey> processChildrenFusion(BTreeNode<TKey> leftChild, BTreeNode<TKey> rightChild);

    protected abstract void fusionWithSibling(TKey sinkKey, BTreeNode<TKey> rightSibling);

    protected abstract TKey transferFromSibling(TKey sinkKey, BTreeNode<TKey> sibling, int borrowIndex);

    /* transforms this node to array of bytes, of length data page length */
    protected abstract byte[] toByteArray() throws IOException;

    /* converts given array bytes of fixed length of our data page to a Node */
    protected abstract BTreeNode<TKey> fromByteArray(byte[] byteArray, int dataPageOffset) throws IOException;
}
