package tuc.org.BTree;

public class Data {
    private int storageByteOffset; // this node is stored at byte index storageByteOffset in the data file. We must calculate the datapage this corresponds to in order to read or write it

    private int data1;
    private int data2;
    private int data3;
    private int data4;

    private boolean dirty;

    public Data() {
        this.data1 = 0;
        this.data2 = 0;
        this.data3 = 0;
        this.data4 = 0;
    }
    public Data(int data1, int data2, int data3, int data4) {
        this.data1 = data1;
        this.data2 = data2;
        this.data3 = data3;
        this.data4 = data4;
    }

    public boolean isDirty() {
        return this.dirty;
    }
    public void setDirty() {
        this.dirty = true;
    }
    public void setStorageByteOffset(int storageByteOffset) {
        this.storageByteOffset = storageByteOffset;
    }
    public int getStorageByteOffset() {
        return this.storageByteOffset;
    }

    @Override
    public String toString() {

        return "data1: "+data1+", data2: "+data2+", data3: "+data3+", data4: "+data4;
    }


	/* takes a Data class, and transforms it to an array of bytes
	  we can't store it as is to the file. We must calculate the data page based on storageByteIndex, load the datapage, replace
	  the part starting from storageByteIndex, and then store the data page back to the file
	  */


    protected byte[] toByteArray() {
        // .....
        // .....
        byte[] byteArray = new byte[32]; // 32: demo size of our data. This should be some constant
        // ..... do stuff
        // ..... do stuff
        return byteArray;

    }


    /*
     this takes a byte array of fixed size, and transforms it to a Data class instance
     it takes the format we store our Data (as specified in toByteArray()) and constructs the Data
     We need as parameter the storageByteIndex in order to set it
     */
    protected Data fromByteArray(byte[] byteArray, int storageByteOffset) {
        Data result = new Data(1,2,3,4); // 1,2,3,4 will be your data extracted from the byte array
        result.setStorageByteOffset(storageByteOffset);

        // ..... do stuff
        // ..... do stuff

        return result;
    }
}
