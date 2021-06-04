package tuc.org.BTree;

import java.io.*;

public class Data {
    private int storageByteOffset; // this node is stored at byte index storageByteOffset in the data file. We must calculate the datapage this corresponds to in order to read or write it
    private static final int DataSize = 32;

    private int data1;
    private int data2;
    private int data3;
    private int data4;
    private int data5;
    private int data6;
    private int data7;
    private int data8;



    private boolean dirty;

    public Data() {
        this.data1 = 0;
        this.data2 = 0;
        this.data3 = 0;
        this.data4 = 0;
        this.data5 = 0;
        this.data6 = 0;
        this.data7 = 0;
        this.data8 = 0;
    }
    public Data(int data1, int data2, int data3, int data4,int data5, int data6, int data7, int data8) {
        this.data1 = data1;
        this.data2 = data2;
        this.data3 = data3;
        this.data4 = data4;
        this.data5 = data5;
        this.data6 = data6;
        this.data7 = data7;
        this.data8 = data8;
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


    protected byte[] toByteArray() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream() ;
        DataOutputStream dos = new DataOutputStream(baos);

        dos.writeInt(data1);
        dos.writeInt(data2);
        dos.writeInt(data3);
        dos.writeInt(data4);
        dos.writeInt(data5);
        dos.writeInt(data6);
        dos.writeInt(data7);
        dos.writeInt(data8);
        dos.close();
        byte[] byteArray = new byte[DataSize];
        byteArray = baos.toByteArray();

        return byteArray;

    }


    /*
     this takes a byte array of fixed size, and transforms it to a Data class instance
     it takes the format we store our Data (as specified in toByteArray()) and constructs the Data
     We need as parameter the storageByteIndex in order to set it
     */
    protected Data fromByteArray(byte[] byteArray, int storageByteOffset) throws IOException {
        DataInputStream dis =new DataInputStream(new ByteArrayInputStream(byteArray));
        Data result = new Data(1,2,3,4,5,6,7,8); // 1,2,3,45,6,7,8 will be your data extracted from the byte array
        result.setStorageByteOffset(storageByteOffset);
        
        result.data1= dis.readInt();
        result.data2=dis.readInt();
        result.data3=dis.readInt();
        result.data4=dis.readInt();
        result.data5=dis.readInt();
        result.data6=dis.readInt();
        result.data7=dis.readInt();
        result.data8=dis.readInt();


        return result;
    }
}
