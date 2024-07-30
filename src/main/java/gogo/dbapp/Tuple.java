package gogo.dbapp;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;


public class Tuple implements Comparable<Tuple>,Serializable {
	private Hashtable<String,Object> tupleContent;
	private String clusteringKeyColumn; //we need it for the compareTo method!
	public Tuple(Hashtable<String,Object> tupleContent, String clusteringKeyColumn) {
		this.tupleContent=tupleContent;
		this.clusteringKeyColumn=clusteringKeyColumn;
	}

	@Override
	public int compareTo(Tuple o) {
		// TODO Auto-generated method stub
        Object k1=this.tupleContent.get(clusteringKeyColumn);
		Object k2= ((Tuple)o).tupleContent.get(clusteringKeyColumn);
		//compare the clustering key of the two tuples based on its type
		if(k1 instanceof Integer) {
			return ((Integer)k1).compareTo((Integer)k2);
		}
		else if(k1 instanceof Double) {
			return ((Double)k1).compareTo((Double)k2);
		}
		else {
			return k1.toString().compareTo(k2.toString());
		}
	}

	public String toString() {
		ArrayList<String>strings=new ArrayList<>();
		for (Map.Entry<String, Object> entry : tupleContent.entrySet()) {
           strings.add(entry.getValue().toString());
        }
		return String.join(",", strings);
		
	}
	public String getClusteringKeyColumn(){
		return clusteringKeyColumn;
	}
	public Hashtable<String,Object> getTupleContent(){
		return tupleContent;
	}


}
