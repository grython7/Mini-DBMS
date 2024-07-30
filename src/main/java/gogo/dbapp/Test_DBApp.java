package gogo.dbapp;
import java.util.ArrayList;
import java.util.Hashtable;

import gogo.btree.BTree;

public class Test_DBApp {
    @SuppressWarnings("rawtypes")
    public static void main(String[]args) throws DBAppException {
        DBApp dbApp = new DBApp();

        // //test inserting into table and see if it reflects to the corresponding indices
            //create table
                Hashtable<String,String> htblColNameType = new Hashtable<>();
                htblColNameType.put("id", "java.lang.Integer");
                htblColNameType.put("name", "java.lang.String");
                htblColNameType.put("gpa", "java.lang.Double");
                htblColNameType.put("major", "java.lang.String");
                dbApp.createTable("Student", "id", htblColNameType);
            //create indices first
                dbApp.createIndex("Student","id", "idIndex");
                dbApp.createIndex("Student","name", "nameIndex");
                dbApp.createIndex("Student","gpa", "gpaIndex");
                dbApp.createIndex("Student","major", "majorIndex");
            //fill arraylist with students to insert
                ArrayList<Hashtable<String,Object>>students=new ArrayList<>();
                fill(students);
             //then insert
                for(Hashtable<String,Object>s:students){
                        dbApp.insertIntoTable("Student", s);
                    }
            //get table from disk and print to check the inserts
                Table student=(Table)HDInterface.deserialize("Student");
                //student.print();
             //print the indices
                BTree idIndex=(BTree)HDInterface.deserialize("Student"+"."+"idIndex");
                // System.out.println("idIndex: ");
                // idIndex.print();
                // System.out.println("******************************");
                BTree nameIndex=(BTree)HDInterface.deserialize("Student"+"."+"nameIndex");
                // System.out.println("nameIndex: ");
                // nameIndex.print();
                // System.out.println("******************************");
                BTree gpaIndex=(BTree)HDInterface.deserialize("Student"+"."+"gpaIndex");
                // System.out.println("gpaIndex: ");
                // gpaIndex.print();
                // System.out.println("******************************");
                BTree majorIndex=(BTree)HDInterface.deserialize("Student"+"."+"majorIndex");
                // System.out.println("majorIndex: ");
                // majorIndex.print();


        // //test update tuple
        //     System.out.println("******************************TESTING UPDATE METHOD******************************");
        //     Hashtable<String,Object>htblColNameValue=new Hashtable<>();
        //     htblColNameValue.put("name", "Caleb_New");
        //     htblColNameValue.put("gpa", 0.6);
        //     // htblColNameValue.put("Type","Animal");
        //     dbApp.updateTable("Student", "40", htblColNameValue);
        //     student=(Table)HDInterface.deserialize("Student");
        //     student.print();
        //     idIndex=(BTree)HDInterface.deserialize("Student"+"."+"idIndex");
        //     System.out.println("idIndex: ");
        //     idIndex.print();
        //     System.out.println("******************************");
        //     nameIndex=(BTree)HDInterface.deserialize("Student"+"."+"nameIndex");
        //     System.out.println("nameIndex: ");
        //     nameIndex.print();
        //     gpaIndex=(BTree)HDInterface.deserialize("Student"+"."+"gpaIndex");
        //     System.out.println("gpaIndex: ");
        //     gpaIndex.print();


        //test delete tuple
            // System.out.println("******************************TESTING DELETE METHOD******************************");
            // Hashtable<String,Object>htblColNameValue2=new Hashtable<>();
            // //htblColNameValue2.put("id", 19);
            // htblColNameValue2.put("name", "Ali");
            // //htblColNameValue2.put("gpa", 10.0);
            // htblColNameValue2.put("major", "CS");
            // // htblColNameValue2.put("Type", "Cat");
            // dbApp.deleteFromTable("Student", htblColNameValue2);
            // student=(Table)HDInterface.deserialize("Student");
            // // student.print();
            // idIndex=(BTree)HDInterface.deserialize("Student"+"."+"idIndex");
            // // System.out.println("idIndex: ");
            // // idIndex.print();
            // // System.out.println("******************************");
            // nameIndex=(BTree)HDInterface.deserialize("Student"+"."+"nameIndex");
            // // System.out.println("nameIndex: ");
            // // nameIndex.print();
            // // System.out.println("******************************");
            // gpaIndex=(BTree)HDInterface.deserialize("Student"+"."+"gpaIndex");
            // // System.out.println("gpaIndex: ");
            // // gpaIndex.print();
            // // System.out.println("******************************");
            // majorIndex=(BTree)HDInterface.deserialize("Student"+"."+"majorIndex");
            // // System.out.println("majorIndex: ");
            // // majorIndex.print();
        


        // test select method
            System.out.println("******************************TESTING SELECT METHOD******************************");
            //select students named Ali or major=cs and gpa<1.0  
            SQLTerm[] arrSQLTerms; 
            arrSQLTerms = new SQLTerm[3]; 
            arrSQLTerms[0]=new SQLTerm();
            arrSQLTerms[0]._strTableName =  "Student"; 
            arrSQLTerms[0]._strColumnName=  "name"; 
            arrSQLTerms[0]._strOperator  =  ">="; 
            arrSQLTerms[0]._objValue     =  "Omar"; 
            arrSQLTerms[1]=new SQLTerm();
            arrSQLTerms[1]._strTableName =  "Student"; 
            arrSQLTerms[1]._strColumnName=  "gpa"; 
            arrSQLTerms[1]._strOperator  =  "<"; 
            arrSQLTerms[1]._objValue =  1;
            arrSQLTerms[2]=new SQLTerm();
            arrSQLTerms[2]._strTableName =  "Student";
            arrSQLTerms[2]._strColumnName=  "major";
            arrSQLTerms[2]._strOperator  =  "=";
            arrSQLTerms[2]._objValue     =  "CS";
            

            String[]strarrOperators = new String[2]; 
            strarrOperators[0] = "OR"; 
            strarrOperators[1] = "AND";

            java.util.Iterator result = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
            while(result.hasNext()){
                System.out.println(result.next());
            }

    }
   
    
    public static void fill(ArrayList<Hashtable<String,Object>> arr){
        //Add 18 students
        //IDs: 1,15,30,40,50,60,47,19,23,2,3,4,24,25,26,48,49,65
        Hashtable<String,Object>Student1 = new Hashtable<>();
        Student1.put("id", 1);
        Student1.put("name", "Ali");
        Student1.put("gpa", 1.0);
        Student1.put("major", "CS");

        Hashtable<String,Object>Student2 = new Hashtable<>();
        Student2.put("id", 15);
        Student2.put("name", "Ahmed");
        Student2.put("gpa", 0.65);
        Student2.put("major", "CS");

        Hashtable<String,Object>Student3 = new Hashtable<>();
        Student3.put("id", 30);
        Student3.put("name", "Ahmed");
        Student3.put("gpa", 0.55);
        Student3.put("major", "CS");

        Hashtable<String,Object>Student4 = new Hashtable<>();
        Student4.put("id", 40);
        Student4.put("name", "Caleb");
        Student4.put("gpa", 0.65);
        Student4.put("major", "Media");

        Hashtable<String,Object>Student5 = new Hashtable<>();
        Student5.put("id", 50);
        Student5.put("name", "Baher");
        Student5.put("gpa", 0.55);
        Student5.put("major", "Media");

        Hashtable<String,Object>Student6 = new Hashtable<>();
        Student6.put("id", 60);
        Student6.put("name", "Hassan");
        Student6.put("gpa", 0.75);
        Student6.put("major", "Materials");

        Hashtable<String,Object>Student7 = new Hashtable<>();
        Student7.put("id", 47);
        Student7.put("name", "Dalida");
        Student7.put("gpa", 0.05);
        Student7.put("major", "Arch");


        Hashtable<String,Object>Student8 = new Hashtable<>();
        Student8.put("id", 19);
        Student8.put("name", "Ali");
        Student8.put("gpa",1.0);
        Student8.put("major", "Arch");

        Hashtable<String,Object>Student9 = new Hashtable<>();
        Student9.put("id", 23);
        Student9.put("name", "Eman");
        Student9.put("gpa", 0.45);
        Student9.put("major", "Accounting");

        Hashtable<String,Object>Student10 = new Hashtable<>();
        Student10.put("id", 2);
        Student10.put("name", "Fady");
        Student10.put("gpa", 0.35);
        Student10.put("major", "Accounting");

        Hashtable<String,Object>Student11 = new Hashtable<>();
        Student11.put("id", 3);
        Student11.put("name", "Hytham");
        Student11.put("gpa", 0.7);
        Student11.put("major", "Law");

        Hashtable<String,Object>Student12 = new Hashtable<>();
        Student12.put("id", 4);
        Student12.put("name", "Mohy");
        Student12.put("gpa", 0.7);
        Student12.put("major", "Law");
        
        Hashtable<String,Object>Student13 = new Hashtable<>();
        Student13.put("id", 24);
        Student13.put("name", "Dalida");
        Student13.put("gpa", 0.7);
        Student13.put("major", "CS");

        Hashtable<String,Object>Student14 = new Hashtable<>();
        Student14.put("id", 25);
        Student14.put("name", "Ali");
        Student14.put("gpa", 1.0);
        Student14.put("major", "Pharma");

        Hashtable<String,Object>Student15 = new Hashtable<>();
        Student15.put("id", 26);
        Student15.put("name", "Kyne");
        Student15.put("gpa", 0.7);
        Student15.put("major", "Law");

        Hashtable<String,Object>Student16 = new Hashtable<>();
        Student16.put("id", 48);
        Student16.put("name", "Omar");
        Student16.put("gpa", 0.7);
        Student16.put("major", "CS");

        Hashtable<String,Object>Student17 = new Hashtable<>();
        Student17.put("id", 49);
        Student17.put("name", "Paula");
        Student17.put("gpa", 0.7);
        Student17.put("major", "Mecha");

        Hashtable<String,Object>Student18 = new Hashtable<>();
        Student18.put("id", 65);
        Student18.put("name", "Lara");
        Student18.put("gpa", 0.7);
        Student18.put("major", "Mecha");



        arr.add(Student1);
        arr.add(Student2);
        arr.add(Student3);
        arr.add(Student4);
        arr.add(Student5);
        arr.add(Student6);
        arr.add(Student7);
        arr.add(Student8);
        arr.add(Student9);
        arr.add(Student10);
        arr.add(Student11);
        arr.add(Student12);
        arr.add(Student13);
        arr.add(Student14);
        arr.add(Student15);
        arr.add(Student16);
        arr.add(Student17);
        arr.add(Student18);


    }

}
