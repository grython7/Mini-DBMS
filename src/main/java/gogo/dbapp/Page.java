package gogo.dbapp;

import java.util.Collections;
import java.util.Vector;
import java.io.Serializable;
import java.util.Properties;
import java.io.FileInputStream;

public class Page implements Serializable {
	Vector<Tuple> tuples;
	String pageName;
	int maxTuples;
	public Page(String pageName) throws DBAppException {
		tuples=new Vector<>();
		this.pageName=pageName;
		Properties prop = new Properties();
		String fileName = "src/main/resources/DBApp.config";
		try (FileInputStream fis = new FileInputStream(fileName)) {
			prop.load(fis);
		} catch (Exception e) {
			throw new DBAppException(e.getLocalizedMessage());
		}
		maxTuples=Integer.parseInt(prop.getProperty("MaxRowCountInPage"));
		// System.out.println("Page created with name: "+pageName+" and max tuples: "+maxTuples);
	}
	
	//returns whether the page contains tuple with the given key value
	public boolean contains(String keyColumn,Object keyValue) {
		for(Tuple t:tuples) {
			if(t.getTupleContent().get(keyColumn).toString().equals(keyValue.toString()))
				return true;
		}
		return false;
	}

	//returns if page is full
	public boolean isFull() {
		return tuples.size()==maxTuples;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Tuple searchByClusteringKeyVal(Object o) {
		// o is the value of the clustering key in search
		//binary search 
		int lo=0;
		int hi=tuples.size()-1;
		int mid;
		while(lo<=hi){
			mid=lo+(hi-lo)/2;
			Tuple t=tuples.get(mid);
			Comparable currentTupleClusteringKeyVal= (Comparable)(t.getTupleContent().get(t.getClusteringKeyColumn()));
			if(((Comparable)o).compareTo(currentTupleClusteringKeyVal) > 0){
				lo=mid+1;
			}
			else if(((Comparable)o).compareTo(currentTupleClusteringKeyVal) < 0){
				hi=mid-1;
			}
			else{
				return tuples.get(mid);
			}
		}
		return null;
	}

	//inserts a tuple in the page in the correct order based on the clustering key
	public void insert(Tuple t) {
		int index=Collections.binarySearch(tuples, t);
		if(index<0) {
			index=-index-1;
		}
		tuples.add(index, t);
	}

	//deletes a tuple from the page
	public void delete(Tuple t) {
		int index=Collections.binarySearch(tuples, t);
		tuples.removeElementAt(index);
	}

	public String toString() {
		String result="";
		int i=0;
		for(Tuple t:tuples) {
			if(i!=tuples.size()-1)
				result+=t.toString()+",\n";
			else
				result+=t.toString();
			i++;
		}
		return result;
	}

	public Vector<Tuple> getTuples() {
		return tuples;
	}

	public Page split(String newPageName) throws DBAppException {
		Page newPage=new Page(newPageName);
		int half=tuples.size()/2;
		while(tuples.size()>half) {
			newPage.insert(tuples.remove(tuples.size()-1));
		}
		return newPage;
	}

	public Object getMinClusteringKeyValue(){
		return tuples.get(0).getTupleContent().get(tuples.get(0).getClusteringKeyColumn());
	}
	public Object getMaxClusteringKeyValue(){
		return tuples.get(tuples.size()-1).getTupleContent().get(tuples.get(tuples.size()-1).getClusteringKeyColumn());
	}

	public String getPageName() {
		return pageName;
	}
	public String setPageName(String pageName) {
		return this.pageName=pageName;
	}


}
