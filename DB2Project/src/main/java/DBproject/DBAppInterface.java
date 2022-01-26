package DBproject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.util.Hashtable;
import java.util.Iterator;

public interface DBAppInterface {

    void init();

    void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException;

    void createIndex(String tableName, String[] columnNames) throws DBAppException;

    void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException, IOException;

    void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException, ParseException, IOException;

    void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException, IOException, ParseException;

    Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException;


}
