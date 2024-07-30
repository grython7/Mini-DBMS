package gogo.dbapp;

/** * @author Wael Abouelsaadat */

public class SQLTerm {

	public String _strTableName,_strColumnName, _strOperator;
	public Object _objValue;

	public SQLTerm(  ){
		
	}
	public String toString(){
		return "{"+_strColumnName+_strOperator+_objValue+"}";
	}

}