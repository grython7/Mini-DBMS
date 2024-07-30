package gogo.dbapp;

import java.io.*;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Vector;
import java.util.HashSet;

import gogo.btree.*;

public class Table implements Serializable{

    private String tableName;
    private String clusteringKeyColumn;
    private Vector<String> pages; //arraylist to include all table's pages names on disk
    private ArrayList<Object[]>pagesMinMax; //arraylist to include all pages' min and max values of clustering key column to do binary search on them
    private Hashtable<String,String> htblColIndexName; //hashtable to include all existing names of indices of the table
    private int ctr=0;  //counter for splitting pages naming
  
    //constructor to create a table
    public Table(String tableName, String clusteringKeyColumn) throws DBAppException{
        this.tableName = tableName;
        //check if table already exists
        File file = new File("src/main/resources/data/"+tableName+".ser");
        if(file.exists())
            throw new DBAppException("Table "+tableName+" already exists");
        this.clusteringKeyColumn = clusteringKeyColumn;
        pages=new Vector<String>();
        pagesMinMax=new ArrayList<Object[]>();
        htblColIndexName=new Hashtable<>();
        //serialize table after creation
        try {
        	HDInterface.serialize(tableName,this);
        }catch (Exception e) {
        	throw new DBAppException("Error in serializing table");
        }
    }
    
    //reads the metadata.csv and returns hashtable of column names and their corresponding data types
    public Hashtable<String,String> getColNameDataType() throws DBAppException{
        Hashtable<String,String> htblColNameDataType = new Hashtable<>();
        try {
            BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            String line;
            //Table Name, Column Name, Column Type, ClusteringKey, IndexName, IndexType
            while ((line = br.readLine()) != null) {
                String[] values = line.split(", ");
                if(values[0].equals(tableName)){
                    htblColNameDataType.put(values[1], values[2]);
                }
            }
            br.close();
        }catch(IOException e){
            throw new DBAppException("Error in reading metadata.csv");
        }
        return htblColNameDataType;
    }

    //creates an index on a column of the table
    @SuppressWarnings({"rawtypes" })
    public void createIndex(String strColName, String strIndexName) throws DBAppException {
        //get table's columns and their data types
        Hashtable<String,String> htblColNameDataType = getColNameDataType();

        //check if column exists in table
        if(!htblColNameDataType.containsKey(strColName))
                 throw new DBAppException("Column " +strColName+" does not exist in table");

        //check if index already exists
        if(htblColIndexName.containsKey(strColName))
            throw new DBAppException("Index already exists for column "+strColName);
        
        //create index with appropriate parameterization
        BTree bTree = null;
        switch (htblColNameDataType.get(strColName)){ //get column type
            case "java.lang.Integer":
                bTree = new BTree<Integer, Vector<String>>();
                break;
            case "java.lang.Double":
                bTree = new BTree<Double, Vector<String>>();
                break;
            case "java.lang.String":
                bTree = new BTree<String, Vector<String>>();
                break;
        }
        //loop on all table's pages and add all relevant keys to the index
        for(String page:pages){
            Page p=(Page)HDInterface.deserialize(page);
            for(Tuple t:p.getTuples()){
                bTree.addValToVectorOfValues(t.getTupleContent().get(strColName), page);
            }
        }

        //add index to list of existing indices of table
        htblColIndexName.put(strColName, strIndexName);

        try {
            //serialize the index
            HDInterface.serialize(tableName+"."+strIndexName, bTree);

            //update metadata.csv
            BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            String line;
            String newLine;
            String content = "";
            while ((line = br.readLine()) != null) {
                String[] values = line.split(", ");
                //Table Name, Column Name, Column Type, ClusteringKey, IndexName,IndexType
                if(values[0].equals(tableName) && values[1].equals(strColName)){
                    newLine = values[0]+", "+values[1]+", "+values[2]+", "+values[3]+", "+strIndexName+", "+"B+tree";
                    content += newLine + "\n";
                }else{
                    content += line + "\n";
                }
            }
            br.close();
            FileWriter fw = new FileWriter("src/main/resources/metadata.csv");
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(content);
            bw.flush();
            bw.close();
        }catch (Exception e) {
            throw new DBAppException("Error in serializing index");
        }

        //serialize the updated table to save the new indexName in the list of existing indices
        HDInterface.serialize(tableName,this);
    }

    //populates the metadata.csv file with table's information
    public void populateMetadata(Hashtable<String,String >htblColNameType) throws DBAppException {
        //Table Name, Column Name, Column Type, ClusteringKey, IndexName,IndexType
        try{
            FileWriter fw = new FileWriter("src/main/resources/metadata.csv", false);
            BufferedWriter bw = new BufferedWriter(fw);
            for (Map.Entry<String, String> entry : htblColNameType.entrySet()) {
                bw.write(tableName+", "+entry.getKey()+", "+entry.getValue()+", "+ clusteringKeyColumn.equals(entry.getKey())+ ", "+ null+", "+null+"\n");
            }
            bw.close();
        }catch(IOException e){
            throw new DBAppException("Error in writing to metadata.csv");
        }
    }

    //This method checks if the data types of the columns in the tuple are correct
    public boolean checkDataTypes(Hashtable<String,Object>  htblColNameValue,
                                  Hashtable<String,String>htblColNameDataType ) throws DBAppException {
        //check if data types all are correct
        for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
            //integer should go with double
            if(htblColNameDataType.get(entry.getKey()).equals("java.lang.Double") && entry.getValue().getClass().getName().equals("java.lang.Integer")){
                //cast integer to double
                htblColNameValue.put(entry.getKey(), (double)(int)entry.getValue());
                continue;
            }
            if(!htblColNameDataType.get(entry.getKey()).equals(entry.getValue().getClass().getName())){
                return false;
            }
        }
        return true;
    }
    
    //returns hashtable of columns that have indices
    @SuppressWarnings("rawtypes")
    public Hashtable<String,Object> getColNameIndex() throws DBAppException {
        Hashtable<String,Object> htblColNameIndexObject = new Hashtable<>();
        for(Map.Entry<String,String> entry:htblColIndexName.entrySet()){
            BTree bTree=(BTree)HDInterface.deserialize(tableName+"."+entry.getValue());
            htblColNameIndexObject.put(entry.getKey(), bTree);
        }
        return htblColNameIndexObject;
    }

    //returns table page that should contain/already contains the given clustering key value using binary search on pagesMinMax arraylist
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Page findPageShouldContainKey(Object clusteringKeyValue) throws DBAppException {
        Page targetPage;
        //check if table is empty
        if(pages.isEmpty()){
            //create first page
            targetPage = new Page(tableName+"_Page@0");
            //add the page to the table
            pages.add(targetPage.getPageName());
            Object[] minmax=new Object[2];
            minmax[0]=clusteringKeyValue;
            minmax[1]=clusteringKeyValue;
            //add the min max values of the page to the arraylist
            pagesMinMax.add(minmax);
            return targetPage;
        }
        
        int lo=0;
        int hi=pagesMinMax.size()-1;
        int mid=0;
        //binary search on ranges of clustering key (PK) in pagesMinMax arraylist to find the correct page to insert the tuple in in Log(n) time
        while(lo<=hi){
            mid=lo+(hi-lo)/2;
            if(((Comparable)clusteringKeyValue).compareTo((Comparable)pagesMinMax.get(mid)[0])<0)
                hi=mid-1;
            else if(((Comparable)clusteringKeyValue).compareTo((Comparable)pagesMinMax.get(mid)[1])>0)
                lo=mid+1;
            else
                break;
        }
 
        if(((Comparable)clusteringKeyValue).compareTo((Comparable)pagesMinMax.get(mid)[0])<0 && mid>0)
            mid--;
    
        targetPage=(Page)HDInterface.deserialize(pages.get(mid));
        return targetPage;
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void deleteTupleFromAllExistingIndices(Page pageWhereTupleExisted, Tuple t, 
     Hashtable<String, Object>htblColNameIndex) throws DBAppException{
            Hashtable<String,Object> htblColNameValue=t.getTupleContent();
            for (Map.Entry<String, Object> entry : htblColNameIndex.entrySet()) {
                BTree bTree = (BTree)entry.getValue();
                Comparable value =(Comparable) htblColNameValue.get(entry.getKey());
                if(!pageWhereTupleExisted.contains(entry.getKey(),value)){
                    ((Vector<String>)bTree.search(value)).remove(pageWhereTupleExisted.getPageName());
                    //delete the key from the index if it has no more values
                    if(((Vector<String>)bTree.search(value)).isEmpty()){
                        bTree.delete(value);
                    }
                }
                //serialize the updated index
                HDInterface.serialize(tableName+"."+htblColIndexName.get(entry.getKey()), bTree);
            }
        }

    //updates all existing indices upon inserting a new tuple
    @SuppressWarnings({ "rawtypes"})
    public void insertTupleToAllExistingIndices(Page pageWhereTupleGotInserted, Tuple t,
                            Hashtable<String, Object>htblColNameIndex) throws DBAppException {

        Hashtable<String,Object> htblColNameValue=t.getTupleContent(); 
        for (Map.Entry<String, Object> entry : htblColNameIndex.entrySet()) {
            BTree bTree = (BTree)entry.getValue();
            bTree.addValToVectorOfValues(htblColNameValue.get(entry.getKey()), pageWhereTupleGotInserted.getPageName());    
            //serialize the updated index
            HDInterface.serialize(tableName+"."+htblColIndexName.get(entry.getKey()), bTree);
        }
    }

    //inserts a new tuple into the table
    public void insert(Hashtable<String,Object>  htblColNameValue) throws DBAppException {
        //get colNameDateType from metadata.csv
        Hashtable<String,String> htblColNameType = getColNameDataType();

        //check if all columns in the tuple exist in the table
        for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
            if(!htblColNameType.containsKey(entry.getKey()))
                throw new DBAppException("Column " +entry.getKey()+" does not exist in table");
        }

        //check if any column in table is not in the tuple
        for (Map.Entry<String, String> entry : htblColNameType.entrySet()) {
            if(!htblColNameValue.containsKey(entry.getKey()))
                throw new DBAppException("Column entry "+ entry.getKey()+ " is missing");
        }

        //check if data types are correct by comparing against metadata.csv
        if(!checkDataTypes(htblColNameValue, htblColNameType)) 
            throw new DBAppException("Data types are not correct");
    
        //get the page where the tuple should be inserted
        Page pageShouldContainKey=findPageShouldContainKey(htblColNameValue.get(clusteringKeyColumn));
        
        //check if a tuple with same clustering key already exists in table
        if(pageShouldContainKey.searchByClusteringKeyVal( htblColNameValue.get(clusteringKeyColumn))!=null)
            throw new DBAppException("Primary key of that value <"+  htblColNameValue.get(clusteringKeyColumn)+"> already exists");

        //create tuple
        Tuple t=new Tuple(htblColNameValue, clusteringKeyColumn);
        
        //get all existing indices 
        Hashtable<String,Object> htblColNameIndexObject = getColNameIndex();

        //in case of overflow, tuple might be inserted in a new page so we need to return the page where the tuple got inserted
        Page pageWhereTupleGotInserted=insertIntoPage(pageShouldContainKey, t, htblColNameIndexObject);

        //update existing indices with the new tuple inserted
        insertTupleToAllExistingIndices(pageWhereTupleGotInserted,t, htblColNameIndexObject);

        //serialize the updated table
        HDInterface.serialize(tableName,this);
    }

    //inserts a tuple into a page and handles the case of page overflow by splitting the page
    private Page insertIntoPage(Page pageShouldContainKey, Tuple t,
                        Hashtable<String,Object> htblColNameIndex) throws DBAppException {

        Page pageWhereTupleGotInserted=pageShouldContainKey;

        if(pageShouldContainKey.isFull()){
            //split the page
            Page oldPage=pageShouldContainKey;
            Page newPage=oldPage.split(tableName+"_Page@"+ ++ctr);
            
            //insert the tuple in the correct page after splitting 
            if(t.compareTo(newPage.getTuples().get(0))<0){
                oldPage.insert(t);
            }
            else{
                newPage.insert(t);   
                pageWhereTupleGotInserted=newPage;
            }

            //add the new page to the table just after the old page
            int newPageIndex=pages.indexOf(oldPage.pageName)+1;
            pages.add(newPageIndex, newPage.getPageName());

            //update the min max values of the old page 
            pagesMinMax.get(pages.indexOf(oldPage.pageName))[0]=oldPage.getMinClusteringKeyValue();
            pagesMinMax.get(pages.indexOf(oldPage.pageName))[1]=oldPage.getMaxClusteringKeyValue();

            //update the min max values of the new page
            Object[] minmax=new Object[2];
            minmax[0]=newPage.getMinClusteringKeyValue();
            minmax[1]=newPage.getMaxClusteringKeyValue();
            pagesMinMax.add(newPageIndex, minmax);

            //serialize the old and new pages
            HDInterface.serialize(oldPage.getPageName(), oldPage);
            HDInterface.serialize(newPage.getPageName(), newPage);


            //update the existing indices for tuples shifted to the new page 
            for(Tuple tuple:newPage.getTuples()){
                if(tuple.compareTo(t)==0) //in case new tuple (not yet indexed) is inserted in the new page
                    continue;
                deleteTupleFromAllExistingIndices(oldPage, tuple, htblColNameIndex);
                insertTupleToAllExistingIndices(newPage, tuple, htblColNameIndex);
            }
         }else{
            //insert the tuple in the page directly
            pageShouldContainKey.insert(t);

            //update the min max values of the page
            pagesMinMax.get(pages.indexOf(pageShouldContainKey.getPageName()))[0]=pageShouldContainKey.getMinClusteringKeyValue();
            pagesMinMax.get(pages.indexOf(pageShouldContainKey.getPageName()))[1]=pageShouldContainKey.getMaxClusteringKeyValue();
            
            //serialize the page
            HDInterface.serialize(pageShouldContainKey.getPageName(), pageShouldContainKey);
        }
        return pageWhereTupleGotInserted;
    }
    
    //updates a tuple in the table
    public void update(String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        Hashtable<String,String> htblColNameDataType = getColNameDataType();
        Object o = null;
        switch (htblColNameDataType.get(clusteringKeyColumn)){ 
            case "java.lang.Integer":
                o = Integer.parseInt(strClusteringKeyValue);
                break;
            case "java.lang.Double":
                o = Double.parseDouble(strClusteringKeyValue);
                break;
            case "java.lang.String":
                o = strClusteringKeyValue;
                break;
        }
        Page page = findPageShouldContainKey(o);
        Tuple t=page.searchByClusteringKeyVal(o);
        if (t == null)
           throw new DBAppException("Tuple with clustering key value <"+strClusteringKeyValue+"> does not exist");

        //check if all columns in the tuple exist in the table
        for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
            if(!htblColNameDataType.containsKey(entry.getKey()))
                throw new DBAppException("Column " +entry.getKey()+" does not exist in table");
        }

        //check if data types are correct by comparing against metadata.csv
        if(!checkDataTypes(htblColNameValue, htblColNameDataType)) 
            throw new DBAppException("Data types are not correct");

        
        //clone the tuple to update it
        Hashtable<String,Object> newTupleContent = new Hashtable<>();
        for (Map.Entry<String, Object> entry : t.getTupleContent().entrySet()) {
            newTupleContent.put(entry.getKey(), entry.getValue());
        }
        Tuple updatedTuple = new Tuple(newTupleContent, clusteringKeyColumn);

        for(Map.Entry<String, Object> entry : htblColNameValue.entrySet()){
            updatedTuple.getTupleContent().put(entry.getKey(), entry.getValue());
        }

        //remove the old tuple from page and all existing indices
        page.delete(t);
        deleteTupleFromAllExistingIndices(page, t, getColNameIndex());
        //insert the updated tuple in the page and all existing indices
        page.insert(updatedTuple);
        insertTupleToAllExistingIndices(page, updatedTuple, getColNameIndex());
        
        //serialize the updated page
        HDInterface.serialize(page.getPageName(), page);
        }
    
     //deletes a tuple from the table
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void delete(Hashtable<String, Object> htblColNameValue) throws DBAppException {
        //if empty htbleColNameValue, delete all tuples
        if(htblColNameValue.isEmpty()){
            for(String pageName:pages){
                Page page = (Page)HDInterface.deserialize(pageName);
                while(!page.getTuples().isEmpty()){
                    Tuple tuple = page.getTuples().get(page.getTuples().size()-1);
                    page.getTuples().removeElementAt((page.getTuples().size()-1));
                    deleteTupleFromAllExistingIndices(page, tuple, getColNameIndex());
                }
                //delete the page from disk
                File file = new File("src/main/resources/"+pageName+".ser");
                file.delete();
            }
            pages.clear();
            pagesMinMax.clear();
            return;
        }
        //check if all columns in the tuple exist in the table
        for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
            if(!getColNameDataType().containsKey(entry.getKey()))
                throw new DBAppException("Column " +entry.getKey()+" does not exist in table");
        }

        //check if data types are correct by comparing against metadata.csv
        if(!checkDataTypes(htblColNameValue, getColNameDataType())) 
            throw new DBAppException("Data types are not correct"); 
        
        //get all indices
        Hashtable<String,Object> htblColNameIndex=getColNameIndex();

        //initialize a flag to check if a tuple is deleted
        boolean deleted=false;

        // check if the primary key is in one of the keys
        if(htblColNameValue.containsKey(clusteringKeyColumn)){
            Page page = findPageShouldContainKey(htblColNameValue.get(clusteringKeyColumn));
            Tuple tuple = page.searchByClusteringKeyVal(htblColNameValue.get(clusteringKeyColumn));
            if(tuple==null){
                System.out.println("No matching tuples found");
                return;
            }
            // check if the rest of the entries match the entries in the tuple
            for(Map.Entry<String, Object> entry : htblColNameValue.entrySet()){
                if(!( tuple.getTupleContent().get(entry.getKey()).toString()).equals(entry.getValue().toString())){
                    System.out.println("No matching tuples found");
                    return;
                }
            }
            page.delete(tuple);
            deleted=true;
            //update the min max values of the page
            pagesMinMax.get(pages.indexOf(page.getPageName()))[0]=page.getMinClusteringKeyValue();
            pagesMinMax.get(pages.indexOf(page.getPageName()))[1]=page.getMaxClusteringKeyValue();
            //delete the tuple from all existing indices
            deleteTupleFromAllExistingIndices(page, tuple, htblColNameIndex);
            //delete page if it is empty
            if(page.getTuples().isEmpty()){
                int index=pages.indexOf(page.getPageName());
                pages.remove(index);
                pagesMinMax.remove(index);
                File file = new File("src/main/resources/"+page.getPageName()+".ser");
                file.delete();
            }
            //serialize the updated page if not empty
            else    
                HDInterface.serialize(page.getPageName(), page);
        }
        //no clusteringKey column in delete 
        else{
            LinkedList<Vector<String>> list=new LinkedList<>();
            //loop on all indices to get all pages that contain entries in the htblColNameValue
            for(Map.Entry<String, Object> entry : htblColNameValue.entrySet()){
                if(htblColNameIndex.containsKey(entry.getKey())){
                    BTree bTree = (BTree)getColNameIndex().get(entry.getKey());
                    Vector<String> pages = (Vector<String>)bTree.search((Comparable)entry.getValue());
                    if(pages==null){
                        System.out.println("No matching tuples found");
                        return;
                    }
                    list.push(pages);
                }
            }

            Vector<String> p;
            if(list.isEmpty())
                p = this.pages;
            else
                p = getIntersectionOfPages(list);

            if(p.isEmpty()){
                System.out.println("No matching tuples found");
                return;
            }
        
            for(String pageName : p){
                Page page = (Page)HDInterface.deserialize(pageName);
                for(int i=0; i<page.getTuples().size(); i++){
                    Tuple tuple = page.getTuples().get(i);
                    boolean flag = true;
                    for(Map.Entry<String, Object> entry : htblColNameValue.entrySet()){
                        if(!( tuple.getTupleContent().get(entry.getKey()).toString()).equals(entry.getValue().toString())){
                            flag = false;
                            break;
                        }
                    }
                    if(flag){
                        page.delete(tuple);
                        deleted=true;
                        i--; //to avoid skipping a tuple
                        
                        //update the min max values of the page
                        pagesMinMax.get(pages.indexOf(page.getPageName()))[0]=page.getMinClusteringKeyValue();
                        pagesMinMax.get(pages.indexOf(page.getPageName()))[1]=page.getMaxClusteringKeyValue();
                        //delete the tuple from all existing indices
                        deleteTupleFromAllExistingIndices(page, tuple, getColNameIndex());
                        //delete page if it is empty
                        if(page.getTuples().isEmpty()){
                            int index=pages.indexOf(page.getPageName());
                            pages.remove(index);
                            pagesMinMax.remove(index);
                            File file = new File("src/main/resources/"+page.getPageName()+".ser");
                            System.out.println(file.delete());
                        }
                        //serialize the updated page if not empty
                        else    
                            HDInterface.serialize(page.getPageName(), page);
                        
                    }
                }
            }
        }
        if(!deleted)
            System.out.println("No matching tuples found");

    }

    //returns the intersection of all pages that contain the entries in the htblColNameValue
    public Vector<String> getIntersectionOfPages(LinkedList<Vector<String>> list) throws DBAppException {
        if(list.isEmpty())
            return new Vector<>();
            
        while(list.size()>1){
            Vector<String> v1 = list.pop();
            HashSet<String> h1 = new HashSet<>(v1);
            Vector<String> v2 = list.pop();
            HashSet<String> h2 = new HashSet<>(v2);
            //convert both lists to hashsets to et intersection by  .retainAll() method
            h1.retainAll(h2);
            v1 = new Vector<>(h1);
            list.push(v1);
        }
        return new Vector<>(list.pop());
    
    }

    //returna the union of all pages that contain the entries in the htblColNameValue
    public Vector<String> getUnionOfPages(LinkedList<Vector<String>> list) throws DBAppException {
        HashSet<String> union = new HashSet<>();
        for(Vector<String> v : list){
            union.addAll(v);
        }
        return new Vector<>(union);
    }

    //returns the XOR of all pages that contain the entries in the htblColNameValue
    public Vector<String> getXOROfPages(LinkedList<Vector<String>> list) throws DBAppException {
        while(list.size()>1){
            Vector<String> v1 = list.pop();
            HashSet<String> h1 = new HashSet<>(v1);
            Vector<String> v2 = list.pop();
            HashSet<String> h2 = new HashSet<>(v2);
            HashSet<String> h3 = new HashSet<>();
            h3.addAll(h1);
            h3.addAll(h2);
            h1.retainAll(h2);
            h3.removeAll(h1);
            list.push(new Vector<>(h3));
        }
        return new Vector<>(list.pop());
    }

    //prints table's name, clustering key column, pagesMinMax and all pages
    public void print() throws DBAppException{
        
        System.out.println("******************************");
        System.out.println("Table name: ".toUpperCase()+tableName);
        System.out.println("Clustering key column: ".toUpperCase()+clusteringKeyColumn);
        System.out.print("Pages MinMax: ".toUpperCase());

        for(Object[]minMax:pagesMinMax)
            System.out.print("["+minMax[0].toString()+"-"+minMax[1].toString()+"] ");
        
        System.out.println("\n******************************");

        //print the table's pages
        for(String page:pages){
            //deserialize the page
            Page p=(Page)HDInterface.deserialize(page);
            System.out.println(page+": ");
            System.out.println(p);
            System.out.println("******************************");
        }
    }

    //checks tuple if matches one selection criterion
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static boolean checkTupleAgainstSQLTerm(SQLTerm term, Tuple tuple){
        String columnName = term._strColumnName;
        Object value = term._objValue;
        String operator = term._strOperator;
        Hashtable<String, Object> tupleData = tuple.getTupleContent();
        Object tupleValue = tupleData.get(columnName);
        switch (operator) {
            case "=":
                if(((Comparable)tupleValue).compareTo(value) == 0){
                    return true;
                }
                break;
            case "!=":
                if(((Comparable)tupleValue).compareTo(value) != 0){
                    return true;
                }
                break;
            case ">":
                if(((Comparable)tupleValue).compareTo(value) > 0){
                    return true;
                }
                break;
            case "<":
                if(((Comparable)tupleValue).compareTo(value) < 0){
                    return true;
                }
                break;
            case ">=":
                if(((Comparable)tupleValue).compareTo(value) >= 0){
                    return true;
                }
                break;
            case "<=":
                if(((Comparable)tupleValue).compareTo(value) <= 0){
                    return true;
                }
                break;
            default:
               throw new IllegalArgumentException("Invalid operator: "+operator);
        }
        return false;
    }
    
     //checks if tuple matches all selection criteria
     public static boolean checkTuple(SQLTerm[] arrSQLTerms, String[]stararrOperators,Tuple tuple){
        LinkedList<Boolean> qBool = new LinkedList<Boolean>();
        LinkedList<String> qOp = new LinkedList<String>();
        for(int i = 0; i < arrSQLTerms.length; i++){
            SQLTerm term = arrSQLTerms[i];
            qBool.add(checkTupleAgainstSQLTerm(term, tuple));
            if(i < stararrOperators.length){
                String operator = stararrOperators[i];
                if(canPush(qOp, operator))
                    qOp.add(operator);
                else{
                    while(!canPush(qOp, operator)){
                        String op = qOp.removeLast();
                        boolean b1 = qBool.removeLast();
                        boolean b2 = qBool.removeLast();
                        if(op.equals("AND")){
                            qBool.add(b1 && b2);
                        }
                        else if(op.equals("OR")){
                            qBool.add(b1 || b2);
                        }
                        else if(op.equals("XOR")){
                            qBool.add(b1 ^ b2);
                        }
                    }
                    qOp.add(operator);
                }
            }
        }
        while(!qOp.isEmpty()){
            String op = qOp.removeLast();
            boolean b1 = qBool.removeLast();
            boolean b2 = qBool.removeLast();
            if(op.equals("AND")){
                qBool.add(b1 && b2);
            }
            else if(op.equals("OR")){
                qBool.add(b1 || b2);
            }
            else if(op.equals("XOR")){
                qBool.add(b1 ^ b2);
            }
        }
        return qBool.removeLast();
    }
    
    //checks if can push operator in stack
    public static boolean canPush(LinkedList<String> q, String operator){
        if(q.isEmpty()){
            return true;
        }
        String lastOp = q.peekLast();
        Hashtable<String, Integer> precedence = new Hashtable<>();
        precedence.put("AND", 1);
        precedence.put("OR", 2);
        precedence.put("XOR", 3);
        if(precedence.get(lastOp) > precedence.get(operator)){
            return true;
        }
        return false;
    }

    //selects tuples from the table based on selection criteria
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Iterator select(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
        //check if all columns in the tuple exist in the table and check their data types
        Hashtable<String,String> htblColNameDataType = getColNameDataType();
        for (SQLTerm term : arrSQLTerms) {
            //check if column exists in table
            if(!htblColNameDataType.containsKey(term._strColumnName))
                throw new DBAppException("Column " +term._strColumnName+" does not exist in table");
            //double should go with integer
            if(htblColNameDataType.get(term._strColumnName).equals("java.lang.Double") && term._objValue.getClass().getName().equals("java.lang.Integer"))
                term._objValue = (double)(int)term._objValue;
            //check if data types are correct
            if(!htblColNameDataType.get(term._strColumnName).equals(term._objValue.getClass().getName()))
                throw new DBAppException("Data types are not correct");
        }
        //check if all operators are valid
        for (String operator : strarrOperators) {
            if(!operator.equals("AND") && !operator.equals("OR") && !operator.equals("XOR"))
                throw new DBAppException("Invalid operator: "+operator);
        }

        LinkedList<Vector<String>> qList=new LinkedList<>(); //stack of vector of pages names retrieved from indices
        LinkedList<String> qOp=new LinkedList<>();

        for(int i=0;i<arrSQLTerms.length;i++){
            SQLTerm term=arrSQLTerms[i];
            Vector<String> p=pages;
            if(htblColIndexName.containsKey(term._strColumnName)){
                BTree bTree=(BTree)HDInterface.deserialize(tableName+"."+htblColIndexName.get(term._strColumnName));
                switch (term._strOperator) {
                    case "=":
                        p=(Vector<String>)bTree.search((Comparable)term._objValue);break;
                    case "!=":
                        p=pages;
                    case ">":
                        p=getUnionOfPages((LinkedList<Vector<String>>)bTree.searchRangeGreaterThan((Comparable)term._objValue, false));break;
                    case "<":
                        p=getUnionOfPages((LinkedList<Vector<String>>)bTree.searchRangeLessThan((Comparable)term._objValue, false));break;
                    case ">=":
                        p=getUnionOfPages((LinkedList<Vector<String>>)bTree.searchRangeGreaterThan((Comparable)term._objValue, true));break;
                    case "<=":
                        p=getUnionOfPages((LinkedList<Vector<String>>)bTree.searchRangeLessThan((Comparable)term._objValue, true));break;
                    default:
                       throw new DBAppException("Invalid operator: "+term._strOperator);
                        }
                }
                qList.push(p);
                if(i<strarrOperators.length){
                    String operator=strarrOperators[i];
                    if(canPush(qOp, operator))
                        qOp.add(operator);
                    else{
                        while(!canPush(qOp, operator)){
                            String op=qOp.removeLast();
                            Vector<String> v1=qList.removeLast();
                            Vector<String> v2=qList.removeLast();
                            LinkedList<Vector<String>> l=new LinkedList<>();
                            l.add(v1);
                            l.add(v2);
                            if(op.equals("AND")){
                                qList.push(getIntersectionOfPages(l));
                            }
                            else if(op.equals("OR")){
                               qList.push(getUnionOfPages(l));
                            }
                            else if(op.equals("XOR")){
                               qList.push(getXOROfPages(l));
                            }
                        }
                        qOp.add(operator);
                    }
                 }
                }
        while(!qOp.isEmpty()){
            String op=qOp.removeLast();
            Vector<String> v1=qList.removeLast();
            Vector<String> v2=qList.removeLast();
            LinkedList<Vector<String>> l=new LinkedList<>();
            l.add(v1);
            l.add(v2);
            if(op.equals("AND")){
                qList.push(getIntersectionOfPages(l));
            }
            else if(op.equals("OR")){
                qList.push(getUnionOfPages(l));
            }
            else if(op.equals("XOR")){
                qList.push(getXOROfPages(l));
            }
        }
        
        Vector<String> pagesToSearchInForSelect= qList.pop();
        
        Vector<Tuple> tuples=new Vector<>();
        for(String pageName:pagesToSearchInForSelect){
            Page page=(Page)HDInterface.deserialize(pageName);
            for(Tuple tuple:page.getTuples()){
                if(checkTuple(arrSQLTerms, strarrOperators, tuple)){
                    tuples.add(tuple);
                }
            }
        }

        return tuples.iterator();
    }
}