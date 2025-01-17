package gogo.dbapp;
/** * @author Wael Abouelsaadat */

import java.util.Hashtable;
import java.util.Iterator;


public class DBApp {



	public DBApp( ){

	}

	// this does whatever initialization you would like
	// or leave it empty if there is no code you want to
	// execute at application startup
	public void init( ){
	}


	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data
	// type as value
	public void createTable(String strTableName,
							String strClusteringKeyColumn,
							Hashtable<String,String> htblColNameType) throws DBAppException {

		Table table=new Table(strTableName,strClusteringKeyColumn);
		table.populateMetadata(htblColNameType);
		//serialize the table
		try {
			HDInterface.serialize(strTableName,table);
		} catch (Exception e) {
			throw new DBAppException("Error in serializing table");
		}


	}
	// following method creates a B+tree index
	public void createIndex(String   strTableName,
							String   strColName,
							String   strIndexName) throws DBAppException {

		Table table=(Table)HDInterface.deserialize(strTableName);
		table.createIndex(strColName,strIndexName);
		//throw new DBAppException("not implemented yet");
	}


	// following method inserts one row only.
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName,
								Hashtable<String,Object>  htblColNameValue) throws DBAppException {

		Table table=(Table)HDInterface.deserialize(strTableName);
		table.insert(htblColNameValue);
		//serialize the table
		try {
			HDInterface.serialize(strTableName,table);
		} catch (Exception e) {
			throw new DBAppException("Error in serializing table");
		}
	}


	// following method updates one row only
	// htblColNameValue holds the key and new value
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName,
							String strClusteringKeyValue,
							Hashtable<String,Object> htblColNameValue)  throws DBAppException {
		Table table=(Table)HDInterface.deserialize(strTableName);
		table.update(strClusteringKeyValue, htblColNameValue);
		try {
			HDInterface.serialize(strTableName,table);
		} catch (Exception e) {
			throw new DBAppException("Error in serializing table");
		}
	}


	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search
	// to identify which rows/tuples to delete.
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName,
								Hashtable<String,Object> htblColNameValue) throws DBAppException {
		Table table=(Table)HDInterface.deserialize(strTableName);
		table.delete(htblColNameValue);
		try {
			HDInterface.serialize(strTableName,table);
		} catch (Exception e) {
			throw new DBAppException("Error in serializing table");
		}
	}


	@SuppressWarnings("rawtypes")
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
                                    String[]  strarrOperators) throws DBAppException {
		String tableName=arrSQLTerms[0]._strTableName;
		Table table=(Table)HDInterface.deserialize(tableName);
		return table.select(arrSQLTerms, strarrOperators);
	}


	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main( String[] args ){

	try{
			String strTableName = "Student";
			DBApp	dbApp = new DBApp( );

			Hashtable htblColNameType = new Hashtable( );
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");
			htblColNameType.put("gpa", "java.lang.Double");
			dbApp.createTable( strTableName, "id", htblColNameType );
			dbApp.createIndex( strTableName, "gpa", "gpaIndex" );

			Hashtable htblColNameValue = new Hashtable( );
			htblColNameValue.put("id",2343432);
			htblColNameValue.put("name", "Ahmed Noor" );
			htblColNameValue.put("gpa",0.95 );
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue.clear( );
			htblColNameValue.put("id", 453455);
			htblColNameValue.put("name", "Ahmed Noor" );
			htblColNameValue.put("gpa", 0.95 );
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue.clear( );
			htblColNameValue.put("id", 5674567);
			htblColNameValue.put("name", "Dalia Noor" );
			htblColNameValue.put("gpa", 1.25 );
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue.clear( );
			htblColNameValue.put("id", 23498);
			htblColNameValue.put("name", "John Noor"  );
			htblColNameValue.put("gpa", 1.5 );
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue.clear( );
			htblColNameValue.put("id", 78452);
			htblColNameValue.put("name", "Zaky Noor");
			htblColNameValue.put("gpa", 0.88 );
			dbApp.insertIntoTable( strTableName , htblColNameValue );


			SQLTerm[] arrSQLTerms;
			arrSQLTerms = new SQLTerm[2];
			arrSQLTerms[0] = new SQLTerm( );
			arrSQLTerms[0]._strTableName =  "Student";
			arrSQLTerms[0]._strColumnName=  "name";
			arrSQLTerms[0]._strOperator  =  "=";
			arrSQLTerms[0]._objValue     =  "John Noor";
			arrSQLTerms[1] = new SQLTerm( );
			arrSQLTerms[1]._strTableName =  "Student";
			arrSQLTerms[1]._strColumnName=  "gpa";
			arrSQLTerms[1]._strOperator  =  "=";
			arrSQLTerms[1]._objValue     = 1.5;

			String[]strarrOperators = new String[1];
			strarrOperators[0] = "OR";
			// select * from Student where name = "John Noor" or gpa = 1.5;
			Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
			while(resultSet.hasNext( )){
				System.out.println(resultSet.next( ));
			}
		}
		catch(Exception exp){
			exp.printStackTrace( );
		}
	}

}