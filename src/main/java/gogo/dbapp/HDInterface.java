package gogo.dbapp;

import java.io.*;

public class HDInterface {
    public static void serialize(String name, Object target) throws DBAppException {
        try {
            FileOutputStream fileOut = new FileOutputStream("src/main/resources/data/"+name+".ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(target);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            System.out.println(i.getMessage());
            throw new DBAppException("Error in serializing object");
        }
    }
    public static Object deserialize(String name) throws DBAppException {
            Object result;
            try {
                FileInputStream fileIn = new FileInputStream("src/main/resources/data/"+name+".ser");
                ObjectInputStream in = new ObjectInputStream(fileIn);
                result = in.readObject();
                in.close();
                fileIn.close();
                return result;
            } catch (Exception c) {
                throw new DBAppException("Error in deserializing object");
                 }
            }
    }
