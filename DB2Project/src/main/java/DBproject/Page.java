package DBproject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Scanner;
import java.util.Vector;

public class Page implements java.io.Serializable {
    int recordNumber;
    int recordNumberMax;
    int pageNumber;
    FileWriter pageCreate;
    String tableName;



    public Page (String tableName, int pageNumber) throws IOException, DBAppException {
        this.recordNumber = 0;
        if (Integer.parseInt(readConfigFile("MaximumRowsCountinPage")) == -1) throw new DBAppException();
            else this.recordNumberMax = Integer.parseInt(readConfigFile("MaximumRowsCountinPage"));
        this.tableName = tableName;
        this.pageNumber = pageNumber;
    }
    public static String readConfigFile(String property) {
        try {
            File configFile = new File(System.getProperty("user.dir") + "/src/main/resources/DBApp.config");
            FileReader reader = new FileReader(configFile);
            Properties props = new Properties();
            props.load(reader);
            String host = props.getProperty(property);
            reader.close();
            return host;
        } catch (FileNotFoundException ex) {
            System.out.println("File Not Found");
            // file does not exist
        } catch (IOException ex) {
            // I/O error
        }
        return "-1";
    }
    public boolean isFull() {
        return recordNumber == recordNumberMax;
    }


}
