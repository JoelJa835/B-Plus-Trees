package tuc.org.BTree;

import java.io.*;

class BTreeLeafNode<TKey extends Comparable<TKey>, TValue> extends BTreeNode<TKey> {
    protected final static int LEAFORDER = 4;
    // private Object[] values;
    // CHANGE FOR STORING ON FILE
    private Integer[] values; // integers pointing to byte offset in data file
    private static final int DataPageSize = 256;

    public BTreeLeafNode() {
        this.keys = new Integer[LEAFORDER + 1];
        this.values = new Integer[LEAFORDER + 1];
    }

    @SuppressWarnings("unchecked")
    public TValue getValue(int index) {
        return (TValue)this.values[index];
    }

    public void setValue(int index, TValue value) {
        this.values[index] = (Integer) value;
        setDirty(); // we changed a value, so this node is dirty and must be flushed to disk
    }

    @Override
    public TreeNodeType getNodeType() {
        return TreeNodeType.LeafNode;
    }

    @Override
    public int search(TKey key) {
        for (int i = 0; i < this.getKeyCount(); ++i) {
            int cmp = this.getKey(i).compareTo(key);
            if (cmp == 0) {
                return i;
            }
            else if (cmp > 0) {
                return -1;
            }
        }

        return -1;
    }


    /* The codes below are used to support insertion operation */

    public void insertKey(TKey key, TValue value) {
        int index = 0;
        while (index < this.getKeyCount() && this.getKey(index).compareTo(key) < 0)
            ++index;
        this.insertAt(index, key, value);
    }

    private void insertAt(int index, TKey key, TValue value) {
        // move space for the new key
        for (int i = this.getKeyCount() - 1; i >= index; --i) {
            this.setKey(i + 1, this.getKey(i));
            this.setValue(i + 1, this.getValue(i));
        }

        // insert new key and value
        this.setKey(index, key);
        this.setValue(index, value);
        // setDirty() will be called in setKey/setValue
        ++this.keyCount;
    }


    /**
     * When splits a leaf node, the middle key is kept on new node and be pushed to parent node.
     */
    @Override
    protected BTreeNode<TKey> split() {
        int midIndex = this.getKeyCount() / 2;

        BTreeLeafNode<TKey, TValue> newRNode = new BTreeLeafNode<TKey, TValue>();
        for (int i = midIndex; i < this.getKeyCount(); ++i) {
            newRNode.setKey(i - midIndex, this.getKey(i));
            newRNode.setValue(i - midIndex, this.getValue(i));
            this.setKey(i, null);
            this.setValue(i, null);
        }
        newRNode.keyCount = this.getKeyCount() - midIndex;
        this.keyCount = midIndex;
        setDirty();// just to make sure

        return newRNode;
    }

    @Override
    protected BTreeNode<TKey> pushUpKey(TKey key, BTreeNode<TKey> leftChild, BTreeNode<TKey> rightNode) {
        throw new UnsupportedOperationException();
    }




    /* The codes below are used to support deletion operation */

    public boolean delete(TKey key) {
        int index = this.search(key);
        if (index == -1)
            return false;

        this.deleteAt(index);
        return true;
    }

    private void deleteAt(int index) {
        int i = index;
        for (i = index; i < this.getKeyCount() - 1; ++i) {
            this.setKey(i, this.getKey(i + 1));
            this.setValue(i, this.getValue(i + 1));
        }
        this.setKey(i, null);
        this.setValue(i, null);
        --this.keyCount;

        // setDirty will be called through setKey/setValue
    }

    @Override
    protected void processChildrenTransfer(BTreeNode<TKey> borrower, BTreeNode<TKey> lender, int borrowIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected BTreeNode<TKey> processChildrenFusion(BTreeNode<TKey> leftChild, BTreeNode<TKey> rightChild) {
        throw new UnsupportedOperationException();
    }

    /**
     * Notice that the key sunk from parent is be abandoned.
     */
    @Override
    @SuppressWarnings("unchecked")
    protected void fusionWithSibling(TKey sinkKey, BTreeNode<TKey> rightSibling) {
        BTreeLeafNode<TKey, TValue> siblingLeaf = (BTreeLeafNode<TKey, TValue>)rightSibling;
        BTreeLeafNode<TKey,TValue> RightNode = (BTreeLeafNode<TKey,TValue>) StorageCache.getInstance().retrieveNode(siblingLeaf.rightSibling);

        int j = this.getKeyCount();
        for (int i = 0; i < siblingLeaf.getKeyCount(); ++i) {
            this.setKey(j + i, siblingLeaf.getKey(i));
            this.setValue(j + i, siblingLeaf.getValue(i));
        }
        this.keyCount += siblingLeaf.getKeyCount();

        this.setRightSibling((BTreeNode<TKey>)StorageCache.getInstance().retrieveNode(siblingLeaf.rightSibling));
        if (siblingLeaf.rightSibling != null)
            RightNode.setLeftSibling(this);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected TKey transferFromSibling(TKey sinkKey, BTreeNode<TKey> sibling, int borrowIndex) {
        BTreeLeafNode<TKey, TValue> siblingNode = (BTreeLeafNode<TKey, TValue>)sibling;

        this.insertKey(siblingNode.getKey(borrowIndex), siblingNode.getValue(borrowIndex));
        siblingNode.deleteAt(borrowIndex);
        // setDirty will be called through setKey/setValue in deleteAt
        return borrowIndex == 0 ? sibling.getKey(0) : this.getKey(0);
    }

    protected byte[] toByteArray() throws IOException {
        // very similar to BTreeInnerNode. Instead of pointers to children (offset to our data pages in our node file), we have pointers
        // to data (byte offset to our data file)

        ByteArrayOutputStream baos = new ByteArrayOutputStream() ;
        DataOutputStream dos = new DataOutputStream(baos);


        dos.writeInt(2);
        if(this.leftSibling == null)
            dos.writeInt(-1);
        else
            dos.writeInt(this.leftSibling);

        if(this.rightSibling == null)
            dos.writeInt(-1);
        else
            dos.writeInt(this.rightSibling);

        if(this.parentNode == null)
            dos.writeInt(-1);
        else
            dos.writeInt(this.parentNode);
        dos.writeInt(this.keyCount);

        for(int i=0; i<this.values.length;i++)
            if(this.values[i]== null)
                dos.writeInt(-1);
            else
                dos.writeInt(this.values[i]);

        for(int i=0; i<this.keys.length;i++)
            if(this.keys[i]== null)
                dos.writeInt(-1);
            else
                dos.writeInt(this.keys[i]);

        byte[] byteArray = new byte[DataPageSize];

        byteArray = baos.toByteArray();

        return byteArray;

    }
    protected BTreeLeafNode<TKey, TValue> fromByteArray(byte[] byteArray, int dataPageOffset) throws IOException {
        // this takes a byte array of fixed size, and transforms it to a BTreeLeafNode
        // it takes the format we store our node (as specified in toByteArray()) and constructs the BTreeLeafNode
        // We need as parameter the dataPageOffset in order to set it
        DataInputStream dis =new DataInputStream(new ByteArrayInputStream(byteArray));
        BTreeLeafNode<TKey, TValue> result = new BTreeLeafNode<TKey, TValue>();
        result.setStorageDataPage(dataPageOffset);

        dis.readInt();
        result.leftSibling = dis.readInt();
        if(result.leftSibling==-1)
            result.leftSibling=null;
        result.rightSibling = dis.readInt();
        if(result.rightSibling==-1)
            result.rightSibling=null;
        result.parentNode = dis.readInt();
        if(result.parentNode==-1)
            result.parentNode=null;

        result.keyCount = dis.readInt();

        for(int i=0; i<this.values.length;i++) {
            result.values[i] = dis.readInt();
            if (result.values[i] == -1)
                result.values[i] =null;
        }
        for(int i=0; i<this.keys.length;i++) {
            result.keys[i] = dis.readInt();
            if (result.keys[i] == -1)
                result.keys[i] = null;
        }


        return result;
    }
}