package tuc.org.BTree;

import java.io.*;
import java.util.HashMap;

public class StorageCache {
    private static final String NODE_STORAGE_FILENAME = "Index.bin";
    private static final String DATA_STORAGE_FILENAME = "Data.bin";
    private static final int DataPageSize = 256;

    private static StorageCache instance;

    private static HashMap retrievedNodes = null;
    private static HashMap retrievedDatas = null;

    // make this private so that noone can create instances of this class
    private StorageCache() {

    }

    private void cacheNode(int dataPageIndex, BTreeNode node) {
        if (StorageCache.retrievedNodes == null) {
            StorageCache.retrievedNodes = new HashMap();
        }
        StorageCache.retrievedNodes.put(dataPageIndex, node);
    }
    private void cacheData(int dataByteOffset, Data data) {
        if (StorageCache.retrievedDatas == null) {
            StorageCache.retrievedDatas = new HashMap();
        }
        StorageCache.retrievedDatas.put(dataByteOffset, data);
    }

    private BTreeNode getNodeFromCache(int dataPageIndex) {
        if (StorageCache.retrievedNodes == null) {
            return null;
        }

        return (BTreeNode)StorageCache.retrievedNodes.get(dataPageIndex);
    }
    private Data getDataFromCache(int dataByteOffset) {
        if (StorageCache.retrievedDatas == null) {
            return null;
        }

        return (Data)StorageCache.retrievedDatas.get(dataByteOffset);
    }

    public static StorageCache getInstance() {
        if (StorageCache.instance == null) {
            StorageCache.instance = new StorageCache();
        }
        return StorageCache.instance;
    }

    public void flush() throws IOException {
        flushNodes();
        flushData();
    }

    // checks each node in retrievedNodes whether it is dirty
    // If they are dirty, writes them to disk
    private void flushNodes() throws IOException {
        BTreeNode node;
        for ( Object dataPageIndex : StorageCache.retrievedNodes.keySet() ) {
            node = (BTreeNode)StorageCache.retrievedNodes.get(dataPageIndex);
            if (node.isDirty()) {
                byte[] byteArray = node.toByteArray();
                File aFile = new File(NODE_STORAGE_FILENAME);								//Creating an object of type File.
                RandomAccessFile myFile = new RandomAccessFile(aFile,"rw");

                // seek to position DATA_PAGE_SIZE * dataPageIndex
                //myFile.seek((long) DataPageSize *dataPageIndex);
                // store byteArray to node/index file at byte position dataPageIndex * DATA_PAGE_SIZE
                myFile.write(byteArray);

                // ******************************
                // we just wrote a data page to our file. This is a good location to increase our counter!!!!!
                // ******************************

            }
        }

        // reset it
        StorageCache.retrievedNodes = null;
    }


    private void flushData() throws IOException {
        Data data;
        int dataPageIndex;

        for ( Object storageByteOffset : StorageCache.retrievedDatas.keySet() ) {
            data = (Data)StorageCache.retrievedDatas.get(storageByteOffset);
            if (data.isDirty()) {
                // data.storageByteIndex tells us at which byte offset in the data file this data is stored
                // From this value, and knowing our data page size, we can calculate the dataPageIndex of the data page in the data file
                // This process may result in writing each data page multiple times if it contains multiple dirty Datas

                byte[] byteArray = data.toByteArray();
                // read datapage given by calculated dataPageIndex from data file
                // copy byteArray to correct position of read bytes
                // store it again to file
                // ......
                // ......
                // ......

                // ******************************
                // we just wrote a data page to our file. This is a good location to increase our counter!!!!!
                // ******************************

            }
        }

        // reset it
        StorageCache.retrievedDatas = null;
    }


    public BTreeNode retrieveNode(int dataPageIndex) throws IOException {
        // if we have this dataPageIndex already in the cache, return it
        BTreeNode result = this.getNodeFromCache(dataPageIndex);
        if (result != null) {
            return result;
        }

        // OPTIONAL, not important for this assignment
        // during a range search, we will potentially retrieve a large set of nodes, despite we will use them only once
        // We can optionally add here a case where "large" number of cached, NOT DIRTY (!) nodes, are removed from memory
        if (StorageCache.retrievedNodes != null && StorageCache.retrievedNodes.keySet().size() > 200) { // we do not want to have more than 100 nodes in cache
            BTreeNode node;
            for ( Object key : StorageCache.retrievedNodes.keySet() ) {
                node = (BTreeNode)StorageCache.retrievedNodes.get(dataPageIndex);
                if (!node.isDirty()) {
                    StorageCache.retrievedNodes.remove(key);
                }
            }
        }
        // open our node/index file
        File aFile = new File(NODE_STORAGE_FILENAME);								//Creating an object of type File.
        RandomAccessFile myFile = new RandomAccessFile(aFile,"rw");

        // seek to position DATA_PAGE_SIZE * dataPageIndex
        myFile.seek(DataPageSize*dataPageIndex);

        // read DATA_PAGE_SIZE bytes
         byte[] pageBytes = new byte[DataPageSize];
         myFile.read(pageBytes);

        // a 4 byte int should tell us what kind of node this is. See toByteArray(). Is it a BTreeInnerNode or a BTreeLeafNode?
        DataInputStream dis =new DataInputStream(new ByteArrayInputStream(pageBytes));
         int type = dis.readInt();

        // if type corresponds to inner node
        if(type == 1) {
            result = new BTreeInnerNode();
            result = result.fromByteArray(pageBytes, dataPageIndex);
        }
        else {
            result = new BTreeLeafNode();
            result = result.fromByteArray(pageBytes, dataPageIndex);
        }


        // ******************************
        // we just read a data page from our file. This is a good location to increase our counter!!!!!
        // ******************************


        // before returning it, cache it for future reference
        this.cacheNode(dataPageIndex, result);


        return result;

    }



    public Data retrieveData(int dataByteOffset) throws IOException {
        // if we have this dataPageIndex already in the cache, return it
        Data result = this.getDataFromCache(dataByteOffset);
        if (result != null) {
            return result;
        }

        // OPTIONAL, not important for this assignment
        // during a range search, we will potentially retrieve a large set of datas, despite we will use them only once
        // We can optionally add here a case where "large" number of cached, NOT DIRTY (!) datas, are removed from memory
        if (StorageCache.retrievedDatas != null && StorageCache.retrievedDatas.keySet().size() > 100) { // we do not want to have more than 100 datas in cache
            Data data;
            for ( Object key : StorageCache.retrievedDatas.keySet() ) {
                data = (Data)StorageCache.retrievedDatas.get(dataByteOffset);
                if (!data.isDirty()) {
                    StorageCache.retrievedDatas.remove(key);
                }
            }
        }


        // open our data file
        File aFile = new File(DATA_STORAGE_FILENAME);								//Creating an object of type File.
        RandomAccessFile myFile = new RandomAccessFile(aFile,"rw");

        // seek to position of the data page that corresponds to dataByteOffset
        myFile.seek(dataByteOffset);

        // read DATA_PAGE_SIZE bytes (some constant)
        byte[] pageBytes = new byte[DataPageSize];
        myFile.read(pageBytes);

        // get the part of the bytes that corresponds to dataByteOffset (--> pageBytesData), and transform to a Data instance
        result = new Data();
        //result = result.fromByteArray(pageBytesData, dataByteOffset);


        // ******************************
        // we just read a data page from our file. This is a good location to increase our counter!!!!!
        // ******************************


        // before returning it, cache it for future reference
        this.cacheData(dataByteOffset, result);


        return result;

    }

    public BTreeInnerNode newInnerNode() {
        BTreeInnerNode result = new BTreeInnerNode();
        this.aquireNodeStorage(result);
        result.setDirty();
        this.cacheNode(result.getStorageDataPage(), result);
        return result;
    }
    public BTreeLeafNode newLeafNode() {
        BTreeLeafNode result = new BTreeLeafNode();
        this.aquireNodeStorage(result);
        result.setDirty();
        this.cacheNode(result.getStorageDataPage(), result);
        return result;
    }

    // opens our node/index file, calculates the dataPageIndex that corresponds to the end of the file (raf.length())
    // and sets it on given node
    private void aquireNodeStorage(BTreeNode node) {
        int dataPageIndex = 0;
        // open file, get length, and calculate the  dataPageIndex that corresponds to the next data page at the end of the file
        // Actually write DATA_PAGE_LENGTH bytes to the end file, so for that subsequent new nodes the new length is used
        node.setStorageDataPage(dataPageIndex);
    }

    public int newData(Data result, int nextFreeDatafileByteOffset) {
        int NO_OF_DATA_BYTES = 32;
        result.setStorageByteOffset(nextFreeDatafileByteOffset);
        result.setDirty(); // so that it will written to disk at next flush
        this.cacheData(result.getStorageByteOffset(), result);
        return nextFreeDatafileByteOffset + NO_OF_DATA_BYTES;
    }
}
