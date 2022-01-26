package DBproject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;

public class DBApp implements DBAppInterface, Serializable {
    Page p;
    int pageNumber = 1;
    Vector<Object> serializer = new Vector<>();
    Vector primaryKeyList;
    Vector primaryKeys;
    String clusteringKey = "";
    boolean foundSearchUpdate = false;
    static String DataPath = System.getProperty("user.dir") + "/src/main/resources/Data";
    String path = System.getProperty("user.dir") + "/src/main/resources/Data/";
    Vector<Integer> rangeVector = new Vector<>();
    int counterTemp = 0;
    int[] globalRangeArray = new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    Vector<int[]> globalVectorOfCombinations = new Vector<>();
    Vector<String> trulyFinalResult = new Vector<>();

    public DBApp() {

    }

    @Override
    public void init() {
        try {
            Path path = Paths.get(DataPath);
            File file = new File(DataPath);
            if (!file.exists()) {
                Files.createDirectories(path);
                System.out.println("Data Directory is created!");
            } else
                System.out.println("Data Directory is already created");
        } catch (IOException e) {
            System.err.println("Failed to create Data directory!" + e.getMessage());
        }
        try {
            String header = "Table Name,Column Name,Column Type,ClusteringKey,Indexed,min,max" + "\n";
            String metaDataPath = System.getProperty("user.dir") + "/src/main/resources/" + "metadata.csv";
            File file = new File(metaDataPath);
            if (!file.exists()) {
                FileWriter metaData = new FileWriter(file);
                System.out.println("MetaData is created");
                metaData.write(header);
                metaData.close();
            } else {
                System.out.println("MetaData is already created");
            }
        } catch (IOException e) {

        }
    }

    public void serializePage(String tableName, int pageNo, Vector<Object> page) {

        try {
            String s = tableName + pageNo + ".ser";
            String pathS = DataPath + "/" + s;
            FileOutputStream file = new FileOutputStream(pathS);
            ObjectOutputStream out = new ObjectOutputStream(file);
            out.writeObject(page);
            out.close();
            file.close();
            System.out.println("Serialization complete");

        } catch (IOException ex) {
            System.out.println("IOException is caught");
        }

    }

    public void serializeOverflow(String tableName, int pageNo, int version, Vector<Object> page) {

        try {
            String s = tableName + pageNo + "overflow" + version + ".ser";
            String pathS = DataPath + "/" + s;
            FileOutputStream file = new FileOutputStream(pathS);
            ObjectOutputStream out = new ObjectOutputStream(file);
            out.writeObject(page);
            out.close();
            file.close();
            System.out.println("Object has been serialized");

        } catch (IOException ex) {
            System.out.println("IOException is caught");
        }
    }

    public Vector<Object> deserializePage(String s) {
        Vector<Object> deserialize = new Vector<>();
        try {
            String pathS = DataPath + "/" + s;
            FileInputStream fileIn = new FileInputStream(pathS);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            deserialize = (Vector<Object>) in.readObject();
            in.close();
            fileIn.close();

        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
        }
        return deserialize;
    }

    public String sortHelper(String tableName) throws IOException, DBAppException {
        File newFile = new File(System.getProperty("user.dir") + "/src/main/resources/" + "metadata.csv");
        BufferedReader csvReader = new BufferedReader(new FileReader(newFile));
        String row;
        String[] data;
        String returnValue = "";
        while ((row = csvReader.readLine()) != null) {
            StringBuilder a = new StringBuilder();
            int count = 0;
            data = row.split(",");
            if (data[3].equals("TRUE") && data[0].equals(tableName)) {
                this.clusteringKey = data[1];
                System.out.print("yes");
            }
            while (count < data.length) {
                a.append(data[count]);
                count++;
            }
            if (a.toString().contains(tableName) && a.toString().contains("java.lang.Integer")
                    && a.toString().contains(clusteringKey))
                returnValue = "java.lang.Integer";
            else if (a.toString().contains(tableName) && a.toString().contains("java.lang.String")
                    && a.toString().contains(clusteringKey))
                returnValue = "java.lang.String";
            else if (a.toString().contains(tableName) && a.toString().contains("java.lang.Double")
                    && a.toString().contains(clusteringKey))
                returnValue = "java.lang.Double";
            else if (a.toString().contains(tableName) && a.toString().contains("java.lang.Date")
                    && a.toString().contains(clusteringKey))
                returnValue = "java.lang.Date";
        }
        csvReader.close();
        return returnValue;
    }

    public boolean sortHelper(String tableName, String x) throws IOException, DBAppException {
        File newFile = new File(System.getProperty("user.dir") + "/src/main/resources/" + "metadata.csv");
        BufferedReader csvReader = new BufferedReader(new FileReader(newFile));
        String row;
        String[] data;
        String returnValue = "";
        boolean flag = false;
        while ((row = csvReader.readLine()) != null) {
            data = row.split(",");
            if (data[1].equals(x) && data[0].equals(tableName)) {
                flag = true;
            }
        }
        csvReader.close();
        return flag;
    }

    public void overflowHandler(String tableName, Object tuple, Object element, int pageNum)
            throws IOException, DBAppException {
        int overflowPageNumber = pageNum - 1;
        int versionCounter = 1;
        Vector<Object> deserialized = new Vector<>();
        Vector<Object> deserializedSorted = new Vector<>();
        Vector<Object> pageCreator = new Vector<>();
        String overflowPrimaryKeys = tableName + "0overflow0" + ".ser";
        String overflowString = tableName + overflowPageNumber + "overflow" + versionCounter + ".ser";
        File s = new File(path + overflowString);
        boolean flag = true;
        while (flag) {
            if (s.exists()) {
                p = new Page(tableName, overflowPageNumber);
                primaryKeys = deserializePage(overflowPrimaryKeys);
                primaryKeys.add(element);
                deserialized = deserializePage(overflowString);
                p.recordNumber = deserialized.size();
                if (!p.isFull()) {
                    deserialized.add(tuple);
                    Collections.sort(primaryKeys);
                    for (int i = 0; i < primaryKeys.size(); i++) {
                        for (int j = 0; j < deserialized.size(); j++) {
                            if (deserialized.elementAt(j).toString()
                                    .contains(" " + primaryKeys.elementAt(i).toString() + "\n")
                                    || deserialized.elementAt(j).toString()
                                            .contains(" " + primaryKeys.elementAt(i).toString() + ","))
                                deserializedSorted.add(deserialized.elementAt(j));
                        }
                    }
                    serializeOverflow(tableName, overflowPageNumber, versionCounter, deserializedSorted);
                    serializeOverflow(tableName, 0, 0, primaryKeys);
                    flag = false;
                    pageNumber = 1;
                } else {
                    versionCounter++;
                    overflowString = tableName + overflowPageNumber + "overflow" + versionCounter + ".ser";
                    s = new File(path + overflowString);
                }
            } else {
                pageCreator.add(tuple);
                primaryKeys.add(element);
                serializeOverflow(tableName, overflowPageNumber, versionCounter, pageCreator);
                serializeOverflow(tableName, 0, 0, primaryKeys);
                pageCreator.clear();
                flag = false;
            }
        }
    }

    public void metaDataFileDetector() throws DBAppException {
        File metaFile = new File(System.getProperty("user.dir") + "/src/main/resources/" + "metadata.csv");
        if (!metaFile.exists()) {
            throw new DBAppException();
        }
    }

    public int compareTo(Object a, Object b) {
        int i = 0;
        if (a instanceof Integer && b instanceof Integer)
            i = Integer.compare((int) a, (int) b);
        else if (a instanceof Double && b instanceof Double)
            i = Double.compare((double) a, (double) b);
        else if (a instanceof String && b instanceof String)
            i = ((String) a).compareTo((String) b);
        else if (a instanceof Date && b instanceof Date)
            i = (((Date) a).before((Date) b)) ? -1 : ((a == b) ? 0 : 1);
        return i;
    }

    @Override
    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
            Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
        this.clusteringKey = clusteringKey;
        try {
            init();
            Vector<String> checkDup = readClusteringKeyFromeMetadata(tableName);
            if (checkDup.size() != 0) {
                System.out.println("table already created");
            } else {
                String row = "";
                String metaDataPath = System.getProperty("user.dir") + "/src/main/resources/" + "metadata.csv";
                Scanner sc = new Scanner(new File(metaDataPath));
                while (sc.hasNextLine()) {
                    row = row + sc.nextLine() + "\n";
                }
                sc.close();
                Set<String> keySetSet = colNameType.keySet();
                Vector<String> keySet = new Vector<String>();
                Object[] keySetarr = keySetSet.toArray();
                for (int i = 0; i < keySetarr.length; i++)
                    keySet.add((String) keySetarr[i]);

                Set<String> keySetSetmax = colNameMax.keySet();
                Vector<String> keySetmax = new Vector<String>();
                Object[] keySetarr1 = keySetSetmax.toArray();
                for (int i = 0; i < keySetarr.length; i++)
                    keySetmax.add((String) keySetarr1[i]);

                Set<String> keySetSetmin = colNameMin.keySet();
                Vector<String> keySetmin = new Vector<String>();
                Object[] keySetarr2 = keySetSetmin.toArray();
                for (int i = 0; i < keySetarr.length; i++)
                    keySetmin.add((String) keySetarr2[i]);

                for (int i = 0; i < keySet.size(); i++) {
                    row = row + tableName + "," + keySet.get(i) + "," + colNameType.get(keySet.get(i)) + ",";
                    if (clusteringKey.equals(keySet.get(i)))
                        row = row + "True,";
                    else
                        row = row + "False,";
                    row = row + "False,";
                    for (int j = 0; j < keySetmin.size(); j++) {
                        if (keySetmin.get(j).equals(keySet.get(i)))
                            row = row + colNameMin.get(keySetmin.get(j)) + ",";
                    }
                    for (int j = 0; j < keySetmax.size(); j++) {
                        if (keySetmax.get(j).equals(keySet.get(i)))
                            row = row + colNameMax.get(keySetmax.get(j)) + "\n";
                    }
                }
                File notFinal = new File(metaDataPath);
                if (notFinal.exists()) {
                    FileWriter metaDataFinal = new FileWriter(metaDataPath);
                    metaDataFinal.write(row);
                    metaDataFinal.close();
                }
            }
        } catch (Exception e) {
            System.out.println("Error");
        }
    }

    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {
        Vector<Vector> pages = new Vector<>();
        Vector<String> pages2 = new Vector<>();
        int i = 1;
        int j = 1;
        String s = tableName + i + ".ser";
        String pathS = DataPath + "/" + s;
        File file = new File(pathS);
        if (file.exists()) {
            while (file.exists()) {
                pages.add(deserializePage(s));
                pages2.add(s);
                j = 1;
                s = tableName + i + "overflow" + j + ".ser";
                pathS = DataPath + "/" + s;
                file = new File(pathS);
                while (file.exists()) {
                    pages.add(deserializePage(s));
                    pages2.add(s);
                    j++;
                    s = tableName + i + "overflow" + j + ".ser";
                    pathS = DataPath + "/" + s;
                    file = new File(pathS);
                }
                i++;
                s = tableName + i + ".ser";
                pathS = DataPath + "/" + s;
                file = new File(pathS);
            }
        } else
            System.out.println("Table name Is Not Correct");

        gridIndex indx = new gridIndex(columnNames, tableName, pages, pages2);

    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue)
            throws DBAppException, IOException {
        metaDataFileDetector();
        Set<String> keySetSet = colNameValue.keySet();
        Vector<String> keySet = new Vector<String>();
        Object[] keySetarr = keySetSet.toArray();
        for (int i = 0; i < keySetarr.length; i++)
            keySet.add((String) keySetarr[i]);
        boolean f = false;
        for (int i = 0; i < keySet.size(); i++) {
            if (!sortHelper(tableName, keySet.get(i))) {
                f = true;
                throw new DBAppException();
            }
        }
        try {
            boolean f1 = checkHashTable(tableName, colNameValue);
            if (f == false && f1 == true) {

                Enumeration<String> e = colNameValue.keys();
                String s = tableName + pageNumber + ".ser";
                String primaryKeySorting = tableName + "0" + ".ser";
                File tmpDir = new File(path + s);
                Object a = tableName + ", ";
                Vector<Object> deserialized = new Vector<>();
                Vector<Object> deserializedSorted = new Vector<>();
                Object primaryKeyValue = "";
                boolean flag = true;
                if (sortHelper(tableName).equals("java.lang.Integer")) {
                    primaryKeyList = new Vector<Integer>();
                    primaryKeys = new Vector<Integer>();
                } else if (sortHelper(tableName).equals("java.lang.Double")) {
                    primaryKeyList = new Vector<Double>();
                    primaryKeys = new Vector<Double>();
                } else if (sortHelper(tableName).equals("java.lang.Date")) {
                    primaryKeyList = new Vector<Date>();
                    primaryKeys = new Vector<Date>();
                } else if (sortHelper(tableName).equals("java.lang.String")) {
                    primaryKeyList = new Vector<String>();
                    primaryKeys = new Vector<String>();
                } else
                    throw new DBAppException();

                while (flag) {
                    p = new Page(tableName, pageNumber);
                    if (tmpDir.exists()) {
                        deserialized = deserializePage(s);
                        primaryKeyList = deserializePage(primaryKeySorting);
                        if (!primaryKeyValue.equals(""))
                            primaryKeyList.add(primaryKeyValue);
                        p = new Page(tableName, pageNumber);
                        p.recordNumber = deserialized.size();
                    } else if (!tmpDir.exists() && deserialized.size() > 0) {
                        deserialized.clear();
                    }
                    while (e.hasMoreElements()) {
                        String key = e.nextElement();
                        Object value = colNameValue.get(key);
                        if (!sortHelper(tableName, key))
                            throw new DBAppException();
                        if (key.equals(clusteringKey) && !primaryKeyList.contains(value)
                                && !primaryKeys.contains(value)) {
                            primaryKeyList.add(value);
                            primaryKeyValue = value;
                        } else if ((key.equals(clusteringKey) && primaryKeyList.contains(value))
                                || (key.equals(clusteringKey) && primaryKeys.contains(value)))
                            throw new DBAppException();
                        if (!e.hasMoreElements())
                            a += key + ", " + value + "\n";
                        else
                            a += key + ", " + value + ",";
                    }
                    if (deserialized.size() > 0 && !p.isFull()) {
                        deserialized.add(a);
                        Collections.sort(primaryKeyList);
                        for (int i = 0; i < primaryKeyList.size(); i++) {
                            for (int j = 0; j < deserialized.size(); j++) {
                                if ((deserialized.elementAt(j).toString()
                                        .contains(" " + primaryKeyList.elementAt(i).toString() + "\n")
                                        || deserialized.elementAt(j).toString()
                                                .contains(" " + primaryKeyList.elementAt(i).toString() + ",")))
                                    deserializedSorted.add(deserialized.elementAt(j));
                            }
                        }
                        serializePage(tableName, pageNumber, deserializedSorted);
                        serializePage(tableName, 0, primaryKeyList);
                        flag = false;
                        pageNumber = 1;
                    } else if (p.isFull() && pageNumber > 1
                            && compareTo(primaryKeyList.elementAt(primaryKeyList.size() - 1),
                                    primaryKeyList.elementAt(((pageNumber - 1) * deserialized.size()) - 1)) > 0
                            && compareTo(primaryKeyList.elementAt(primaryKeyList.size() - 1),
                                    primaryKeyList.elementAt(deserialized.size() * (pageNumber - 1))) < 0) {
                        overflowHandler(tableName, a, primaryKeyList.elementAt(primaryKeyList.size() - 1), pageNumber);
                        primaryKeyList.removeElementAt(primaryKeyList.size() - 1);
                        flag = false;
                        pageNumber = 1;
                    } else if (p.isFull() && compareTo(primaryKeyList.elementAt(primaryKeyList.size() - 1),
                            primaryKeyList.elementAt((pageNumber * deserialized.size()) - 1)) < 0) {
                        String x = tableName + (pageNumber + 1) + ".ser";
                        Object store;
                        File detector = new File(path + x);
                        Object primaryKeyDestroyer;
                        int pageNumberHolder = pageNumber + 1;
                        p = new Page(tableName, pageNumberHolder);
                        if (detector.exists()) {
                            store = deserialized.elementAt(deserialized.size() - 1);
                            if (pageNumber > 1)
                                primaryKeyDestroyer = primaryKeyList
                                        .elementAt(((pageNumber - 1) * deserialized.size()) - 1);
                            else
                                primaryKeyDestroyer = primaryKeyList.elementAt(deserialized.size() - 1);
                            deserialized.removeElementAt(deserialized.size() - 1);
                            deserialized.add(a);
                            Collections.sort(primaryKeyList);
                            for (int i = 0; i < primaryKeyList.size(); i++) {
                                for (int j = 0; j < deserialized.size(); j++) {
                                    if (deserialized.elementAt(j).toString()
                                            .contains(" " + primaryKeyList.elementAt(i).toString() + "\n")
                                            || deserialized.elementAt(j).toString()
                                                    .contains(" " + primaryKeyList.elementAt(i).toString() + ","))
                                        deserializedSorted.add(deserialized.elementAt(j));

                                }
                            }
                            serializePage(tableName, pageNumber, deserializedSorted);
                            if (deserializePage(x).size() < p.recordNumberMax) {
                                deserializedSorted.clear();
                                deserialized = deserializePage(x);
                                deserialized.add(store);
                                for (int i = 0; i < primaryKeyList.size(); i++) {
                                    for (int j = 0; j < deserialized.size(); j++) {
                                        if (deserialized.elementAt(j).toString()
                                                .contains(" " + primaryKeyList.elementAt(i).toString() + "\n")
                                                || deserialized.elementAt(j).toString()
                                                        .contains(" " + primaryKeyList.elementAt(i).toString() + ","))
                                            deserializedSorted.add(deserialized.elementAt(j));
                                    }
                                }
                                serializePage(tableName, pageNumber + 1, deserializedSorted);
                                serializePage(tableName, 0, primaryKeyList);
                                flag = false;
                                pageNumber = 1;
                            } else {
                                if (primaryKeyList.contains(primaryKeyDestroyer))
                                    primaryKeyList.removeElement(primaryKeyDestroyer);
                                else
                                    throw new DBAppException();
                                serializePage(tableName, 0, primaryKeyList);
                                overflowHandler(tableName, store, primaryKeyDestroyer, pageNumber + 1);
                                flag = false;
                                pageNumber = 1;
                            }
                        } else {
                            serializer.clear();
                            serializePage(tableName, pageNumber + 1, serializer);
                        }
                    }

                    else if (p.isFull()) {
                        ++pageNumber;
                        s = tableName + pageNumber + ".ser";
                        tmpDir = new File(path + s);
                    }

                    else {
                        serializer.add(a);
                        p = new Page(tableName, pageNumber);
                        serializePage(tableName, pageNumber, serializer);
                        serializePage(tableName, 0, primaryKeyList);
                        serializer.clear();
                        flag = false;
                        pageNumber = 1;
                    }
                }
            }
        } catch (Exception e) {
            throw new DBAppException();
        }
    }

    static Vector<String> readClusteringKeyFromeMetadata(String tableName) {
        Vector<String> r = new Vector<String>();
        String pathMetaData = System.getProperty("user.dir") + "/src/main/resources/" + "metadata.csv";
        try (BufferedReader br = new BufferedReader(new FileReader(pathMetaData))) {
            String line = br.readLine();
            while (line != null) {
                String[] s = line.split(",");
                if ((s[3].trim()).equals("True") && (s[0].trim()).equals(tableName)) {
                    for (int i = 0; i < s.length; i++) {
                        r.add(s[i]);
                    }
                }
                line = br.readLine();
            }
        } catch (IOException e) {
            System.out.println("check meta-data file");
        }
        return r;
    }

    static int binarySearchint(Vector<Integer> vector, int i, int length, int goal) {

        if (length >= i) {
            int mid = i + (length - i) / 2;
            if (vector.get(mid) == goal)
                return mid;
            if (vector.get(mid) > goal)
                return binarySearchint(vector, i, mid - 1, goal);
            return binarySearchint(vector, mid + 1, length, goal);
        }
        return -1;
    }

    static int binarySearchString(Vector<String> vector, String goal) {
        int i = 0, length = vector.size() - 1;
        while (i <= length) {
            int mid = i + (length - i) / 2;
            int res = goal.compareTo(vector.get(mid));
            if (res == 0)
                return mid;
            if (res > 0)
                i = mid + 1;
            else
                length = mid - 1;
        }
        return -1;
    }

    static int binarySearchDouble(Vector<Double> vector, int i, int length, Double goal) {
        int mid = 0;
        while (i <= length) {
            mid = (i + length) / 2;

            if (goal > vector.get(mid))
                i = mid + 1;
            else if (goal < vector.get(mid))
                length = mid - 1;
            else
                break;
        }
        if (i > length)
            return -1;
        else
            return mid;
    }

    static int binarySearchDate(Vector<java.util.Date> vector, java.util.Date goal) {
        int i = 0, length = vector.size() - 1;
        while (i <= length) {
            int mid = i + (length - i) / 2;
            int res = goal.compareTo(vector.get(mid));
            if (res == 0)
                return mid;
            if (res > 0)
                i = mid + 1;
            else
                length = mid - 1;
        }
        return -1;
    }

    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
            throws DBAppException {
        try {
            metaDataFileDetector();
            Set<String> keySetSet = columnNameValue.keySet();
            Vector<String> keySet = new Vector<String>();
            Object[] keySetarr = keySetSet.toArray();
            for (int i = 0; i < keySetarr.length; i++)
                keySet.add((String) keySetarr[i]);
            boolean f = false;
            for (int i = 0; i < keySet.size(); i++) {
                if (!sortHelper(tableName, keySet.get(i))) {
                    f = true;
                    throw new DBAppException();
                }
            }
            boolean f1 = checkHashTable(tableName, columnNameValue);
            if (f == false && f1 == true) {
                // if (f == false) {

                int i = 1;
                int j = 1;
                String s = tableName + i + ".ser";
                String pathS = DataPath + "/" + s;
                File file = new File(pathS);
                foundSearchUpdate = false;
                if (file.exists()) {
                    while (file.exists() && foundSearchUpdate == false) {
                        updateTableHelper(tableName, clusteringKeyValue, columnNameValue, s, i, 0);
                        j = 1;
                        s = tableName + i + "overflow" + j + ".ser";
                        pathS = DataPath + "/" + s;
                        file = new File(pathS);
                        while (file.exists() && foundSearchUpdate == true) {
                            updateTableHelper(tableName, clusteringKeyValue, columnNameValue, s, i, j);
                            j++;
                            s = tableName + i + "overflow" + j + ".ser";
                            pathS = DataPath + "/" + s;
                            file = new File(pathS);
                        }
                        i++;
                        s = tableName + i + ".ser";
                        pathS = DataPath + "/" + s;
                        file = new File(pathS);
                    }
                } else
                    System.out.println("Table name Is Not Correct");
            } else {
                // throw new DBAppException();
            }
        } catch (Exception e) {
            throw new DBAppException();
        }
    }

    public void updateTableHelper(String tableName, String clusteringKeyValue,
            Hashtable<String, Object> columnNameValue, String s, int pageNumber, int overFlow)
            throws DBAppException, ParseException, IOException {
        Vector<Object> deserializePage = deserializePage(s);
        Vector<String> clusteringKeyinfo = readClusteringKeyFromeMetadata(tableName);
        Set<String> keySetSet = columnNameValue.keySet();
        Vector<String> keySet = new Vector<String>();
        Object[] keySetarr = keySetSet.toArray();
        for (int i = 0; i < keySetarr.length; i++)
            keySet.add((String) keySetarr[i]);
        if (clusteringKeyinfo.get(2).equals("java.lang.Double"))
            columnNameValue.put(clusteringKeyinfo.get(1), Double.valueOf(clusteringKeyValue));
        else if (clusteringKeyinfo.get(2).equals("java.lang.Integer"))
            columnNameValue.put(clusteringKeyinfo.get(1), Integer.valueOf(clusteringKeyValue));
        else
            columnNameValue.put(clusteringKeyinfo.get(1), clusteringKeyValue);

        Vector<String> order = getTubleInOrder(tableName);
        Object newValue = tableName + ", ";
        for (int i = 0; i < order.size(); i++) {
            if (i != order.size() - 1)
                newValue = newValue + order.get(i) + ", " + columnNameValue.get(order.get(i)) + ",";
            else
                newValue = newValue + order.get(i) + ", " + columnNameValue.get(order.get(i));

        }
        int index = -1;
        if (clusteringKeyinfo.get(2).equals("java.lang.Integer")) {
            Vector<Integer> deserializePagePrimaryKeys = new Vector<Integer>();
            for (int i = 0; i < deserializePage.size(); i++) {
                int indexOfKey = -1;
                String tuple = (deserializePage.get(i)).toString();
                String[] tupleSplite = tuple.split(",");
                for (int z = 0; z < tupleSplite.length; z++) {
                    tupleSplite[z] = tupleSplite[z].trim();
                    if (tupleSplite[z].equals(clusteringKeyinfo.get(1)))
                        indexOfKey = z + 1;
                }
                deserializePagePrimaryKeys.add(Integer.valueOf(tupleSplite[indexOfKey]));
            }
            index = binarySearchint(deserializePagePrimaryKeys, 0, deserializePagePrimaryKeys.size() - 1,
                    Integer.valueOf(clusteringKeyValue));
            if (index != -1) {
                deserializePage.set(index, newValue);
                foundSearchUpdate = true;
            }
        } else if (clusteringKeyinfo.get(2).equals("java.lang.Double")) {
            Vector<Double> deserializePagePrimaryKeys = new Vector<>();
            for (int i = 0; i < deserializePage.size(); i++) {
                int indexOfKey = -1;
                String tuple = (deserializePage.get(i)).toString();
                String[] tupleSplite = tuple.split(",");
                for (int z = 0; z < tupleSplite.length; z++) {
                    tupleSplite[z] = tupleSplite[z].trim();
                    if (tupleSplite[z].equals(clusteringKeyinfo.get(1)))
                        indexOfKey = z + 1;
                }
                deserializePagePrimaryKeys.add(Double.valueOf(tupleSplite[indexOfKey]));
            }
            index = binarySearchDouble(deserializePagePrimaryKeys, 0, deserializePagePrimaryKeys.size() - 1,
                    Double.valueOf(clusteringKeyValue));
            if (index != -1) {
                deserializePage.set(index, newValue);
                foundSearchUpdate = true;
            }
        } else {
            Vector<String> deserializePagePrimaryKeys = new Vector<String>();
            for (int i = 0; i < deserializePage.size(); i++) {
                int indexOfKey = -1;
                String tuple = (deserializePage.get(i)).toString();
                String[] tupleSplite = tuple.split(",");
                for (int z = 0; z < tupleSplite.length; z++) {
                    tupleSplite[z] = tupleSplite[z].trim();
                    if (tupleSplite[z].equals(clusteringKeyinfo.get(1)))
                        indexOfKey = z + 1;
                }
                deserializePagePrimaryKeys.add((String.valueOf(tupleSplite[indexOfKey])));
            }
            index = binarySearchString(deserializePagePrimaryKeys, clusteringKeyValue);
            if (index != -1) {
                deserializePage.set(index, newValue);
                foundSearchUpdate = true;
            }
        }

        if (overFlow == 0) {
            serializePage(tableName, pageNumber, deserializePage);
        } else {
            serializeOverflow(tableName, pageNumber, overFlow, deserializePage);
        }
    }

    public static Vector<String> getTubleInOrder(String tableName) {
        String metaDataPath = System.getProperty("user.dir") + "/src/main/resources/" + "metadata.csv";
        Scanner sc;
        Vector<String> order = new Vector<String>();
        Boolean found = false;
        try {
            sc = new Scanner(new File(metaDataPath));
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                String[] lineSplite = line.split(",");
                if ((lineSplite[0].trim()).equals(tableName)) {
                    found = true;
                    order.add(lineSplite[1]);
                }
                if (found && !(lineSplite[0].trim()).equals(tableName))
                    break;
            }

            sc.close();
        } catch (Exception e) {
            System.out.println("cannot delete table. table doesnt exist.");
        }
        return order;
    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        String columnName;
        Vector<String> comparisonOperators = new Vector<>();
        String tableName = "";
        Object value;
        boolean flag = false;
        Bucket bucket;
        String fileName = "";
        File grid;
        File file = new File(DataPath + "/gridIndices" + ".ser");
        Vector<String> indices = new Vector<>();
        int originalRange;
        if (file.exists())
            indices = gridIndex.deserialize("gridIndices");
        String[] order = new String[]{};
        Vector<String> colNames = new Vector<>();
        Vector<Object[]> gridVector = new Vector<>();
        Hashtable<String, Object> hash = new Hashtable<>();
        Hashtable<String, String> operatorHash = new Hashtable<>();
        Vector<Vector<Object>> finalResult = new Vector<>();
        Vector<Object> finalResultHelper = new Vector<>();
        Iterator iteratorReturned = null;
        boolean stopper = false;
        Iterator<Object> result = new Iterator<Object>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Object next() {
                return null;
            }
        };
        for (SQLTerm sqlTerm : sqlTerms) {
            columnName = sqlTerm._strColumnName;
            colNames.add(columnName);
            tableName = sqlTerm._strTableName;
            value = sqlTerm._objValue;
            hash.put(columnName, value);
            operatorHash.put(columnName,sqlTerm._strOperator);
        }
        for (int i = 0; i < indices.size(); i++) {
            for (int j = 0; j < colNames.size(); j++) {
                if (!indices.elementAt(i).contains(colNames.elementAt(j)) || indices.isEmpty()) flag = false;
                else flag = true;
            }
            if (flag) {
                fileName = indices.elementAt(i);
                order = indices.elementAt(i).split("-");
            }
        }
        for (String x: order){
            if (!(hash.containsKey(x))) {
                flag = false;
                break;
            }
        }
        if(flag) {
            if (!(fileName.equals("")))
                grid = new File(System.getProperty("user.dir") + "/src/main/resources/Data/" + fileName + ".ser");
            else throw new DBAppException();

            if (grid.exists()) {
                gridVector.add((Object[]) deserializePage(fileName + ".ser").elementAt(0));
                CombinationRepetition(globalRangeArray, globalRangeArray.length, sqlTerms.length);
//            if (globalVectorOfCombinations.contains(new int[]{2,0})) System.out.println(Arrays.toString(globalVectorOfCombinations.elementAt(0)));
                for (int i = 0; i < order.length; i++) {
                    Object val = hash.get(order[i]);
                    gridIndexTraversal(gridVector.elementAt(0), i, order[i], tableName, val, sqlTerms.length);
                }
                if (rangeVector.size() > 0) {
                    int[] rangeVectorConvertedToArray = new int[rangeVector.size()];
                    for (int i = 0; i < rangeVector.size(); i++)
                        rangeVectorConvertedToArray[i] = rangeVector.elementAt(i);
                } else throw new DBAppException();
                for (int z = 0; z < order.length; z++) {
                    switch (operatorHash.get(order[z])) {
                        case "=":
                            for (int q = 0; q < globalVectorOfCombinations.size(); q++) {
                                if (globalVectorOfCombinations.elementAt(q)[z] == rangeVector.elementAt(z)) {
                                    bucket = getBucket(globalVectorOfCombinations.elementAt(q), gridVector.elementAt(0));
//                                        System.out.println(Arrays.toString(globalVectorOfCombinations.elementAt(q)));
//                                        System.out.println(retrieveDataFromBucket(bucket));
                                    Vector<Object> dataRetrieval = retrieveDataFromBucket(bucket);
                                    if (!dataRetrieval.isEmpty()){
                                        for (Object b: dataRetrieval){
                                            String str = b.toString().replaceAll("\\s+", "");
                                            String[] arr = str.split(",");
                                            for (int i = 0; i < arr.length; i++) {
                                                if (arr[i].equals(order[z]))
                                                    if (compareTo(arr[i+1], hash.get(order[z]).toString()) == 0)
                                                        finalResultHelper = finalResultHelper(dataRetrieval, finalResultHelper);
                                            }
                                        }
                                    }
                                }
                            }
                            finalResult.add(new Vector<>());
                            for (Object a : finalResultHelper) finalResult.elementAt(finalResult.size() - 1).add(a);
//                        System.out.print(finalResult);
                            finalResultHelper.clear();
                            break;
                        case "<":
                            for (int q = 0; q < globalVectorOfCombinations.size(); q++) {
                                if (globalVectorOfCombinations.elementAt(q)[z] < rangeVector.elementAt(z)) {
                                    bucket = getBucket(globalVectorOfCombinations.elementAt(q), gridVector.elementAt(0));
//                                        System.out.println(Arrays.toString(globalVectorOfCombinations.elementAt(q)));
//                                        System.out.println(retrieveDataFromBucket(bucket));
                                    Vector<Object> dataRetrieval = retrieveDataFromBucket(bucket);
                                    if (!dataRetrieval.isEmpty()){
                                        for (Object b: dataRetrieval){
                                            String str = b.toString().replaceAll("\\s+", "");
                                            String[] arr = str.split(",");
                                            for (int i = 0; i < arr.length; i++) {
                                                if (arr[i].equals(order[z]))
                                                    if (compareTo(arr[i+1], hash.get(order[z]).toString()) < 0)
                                                        finalResultHelper = finalResultHelper(dataRetrieval, finalResultHelper);
                                            }
                                        }
                                    }
                                }
                            }
                            finalResult.add(new Vector<>());
                            for (Object a : finalResultHelper) finalResult.elementAt(finalResult.size() - 1).add(a);
                            finalResultHelper.clear();
                            break;
                        case "<=":
                            for (int q = 0; q < globalVectorOfCombinations.size(); q++) {
                                if (globalVectorOfCombinations.elementAt(q)[z] <= rangeVector.elementAt(z)) {
                                    bucket = getBucket(globalVectorOfCombinations.elementAt(q), gridVector.elementAt(0));
//                                        System.out.println(Arrays.toString(globalVectorOfCombinations.elementAt(q)));
//                                        System.out.println(retrieveDataFromBucket(bucket));
                                    Vector<Object> dataRetrieval = retrieveDataFromBucket(bucket);
                                    if (!dataRetrieval.isEmpty()){
                                        for (Object b: dataRetrieval){
                                            String str = b.toString().replaceAll("\\s+", "");
                                            String[] arr = str.split(",");
                                            for (int i = 0; i < arr.length; i++) {
                                                if (arr[i].equals(order[z]))
                                                    if (compareTo(arr[i+1], hash.get(order[z]).toString()) < 0 || compareTo(arr[i+1], hash.get(order[z]).toString()) == 0)
                                                        finalResultHelper = finalResultHelper(dataRetrieval, finalResultHelper);
                                            }
                                        }
                                    }
                                }
                            }
                            finalResult.add(new Vector<>());
                            for (Object a : finalResultHelper) finalResult.elementAt(finalResult.size() - 1).add(a);
                            finalResultHelper.clear();
                            break;
                        case ">":
                            for (int q = 0; q < globalVectorOfCombinations.size(); q++) {
                                if (globalVectorOfCombinations.elementAt(q)[z] > rangeVector.elementAt(z)) {
                                    bucket = getBucket(globalVectorOfCombinations.elementAt(q), gridVector.elementAt(0));
//                                        System.out.println(Arrays.toString(globalVectorOfCombinations.elementAt(q)));
//                                        System.out.println(retrieveDataFromBucket(bucket));
                                    Vector<Object> dataRetrieval = retrieveDataFromBucket(bucket);
                                    if (!dataRetrieval.isEmpty()){
                                        for (Object b: dataRetrieval){
                                            String str = b.toString().replaceAll("\\s+", "");
                                            String[] arr = str.split(",");
                                            for (int i = 0; i < arr.length; i++) {
                                                if (arr[i].equals(order[z]))
                                                    if (compareTo(arr[i+1], hash.get(order[z]).toString()) > 0)
                                                        finalResultHelper = finalResultHelper(dataRetrieval, finalResultHelper);
                                            }
                                        }
                                    }
                                }
                            }
                            finalResult.add(new Vector<>());
                            for (Object a : finalResultHelper) finalResult.elementAt(finalResult.size() - 1).add(a);
                            finalResultHelper.clear();
                            break;
                        case ">=":
                            for (int q = 0; q < globalVectorOfCombinations.size(); q++) {
                                if (globalVectorOfCombinations.elementAt(q)[z] >= rangeVector.elementAt(z)) {
                                    bucket = getBucket(globalVectorOfCombinations.elementAt(q), gridVector.elementAt(0));
//                                        System.out.println(Arrays.toString(globalVectorOfCombinations.elementAt(q)));
//                                        System.out.println(retrieveDataFromBucket(bucket));
                                    Vector<Object> dataRetrieval = retrieveDataFromBucket(bucket);
                                    if (!dataRetrieval.isEmpty()){
                                        for (Object b: dataRetrieval){
                                            String str = b.toString().replaceAll("\\s+", "");
                                            String[] arr = str.split(",");
                                            for (int i = 0; i < arr.length; i++) {
                                                if (arr[i].equals(order[z]))
                                                    if (compareTo(arr[i+1], hash.get(order[z]).toString()) > 0 || compareTo(arr[i+1], hash.get(order[z]).toString()) == 0)
                                                        finalResultHelper = finalResultHelper(dataRetrieval, finalResultHelper);
                                            }
                                        }
                                    }
                                }
                            }
                            finalResult.add(new Vector<>());
                            for (Object a : finalResultHelper) finalResult.elementAt(finalResult.size() - 1).add(a);
                            finalResultHelper.clear();
                            break;
                        case "!=":
                            for (int q = 0; q < globalVectorOfCombinations.size(); q++) {
                                if (globalVectorOfCombinations.elementAt(q)[z] != rangeVector.elementAt(z)) {
                                    bucket = getBucket(globalVectorOfCombinations.elementAt(q), gridVector.elementAt(0));
//                                        System.out.println(Arrays.toString(globalVectorOfCombinations.elementAt(q)));
//                                        System.out.println(retrieveDataFromBucket(bucket));
                                    Vector<Object> dataRetrieval = retrieveDataFromBucket(bucket);
                                    if (!dataRetrieval.isEmpty()){
                                        for (Object b: dataRetrieval){
                                            String str = b.toString().replaceAll("\\s+", "");
                                            String[] arr = str.split(",");
                                            for (int i = 0; i < arr.length; i++) {
                                                if (arr[i].equals(order[z]))
                                                    if (compareTo(arr[i+1], hash.get(order[z]).toString()) != 0)
                                                        finalResultHelper = finalResultHelper(dataRetrieval, finalResultHelper);
                                            }
                                        }
                                    }
                                }
                            }
                            finalResult.add(new Vector<>());
                            for (Object a : finalResultHelper) finalResult.elementAt(finalResult.size() - 1).add(a);
                            finalResultHelper.clear();
                            break;

                    }
                }
            }
            if (arrayOperators.length > 1) throw new DBAppException();
            else {
                for (Vector<Object> a : finalResult) {
                    for (Object b : a) {
                        String strFinal = b.toString();
                        String str = b.toString().replaceAll("\\s+", "");
                        String[] arr = str.split(",");
                        boolean finalDetector = true;
                        for (int i = 0; i < arr.length; i++) {
                            if (arrayOperators[0].equals("AND")) {
                                if (hash.containsKey(arr[i])) {
                                    switch (operatorHash.get(arr[i])) {
                                        case "=":
                                            if (!(compareTo(arr[i + 1], hash.get(arr[i]).toString()) == 0))
                                                finalDetector = false;
                                            break;
                                        case "<":
                                            if (!(compareTo(arr[i + 1], hash.get(arr[i]).toString()) < 0))
                                                finalDetector = false;
                                            break;
                                        case "<=":
                                            if (!((compareTo(arr[i + 1], hash.get(arr[i]).toString()) < 0) || compareTo(arr[i + 1], hash.get(arr[i]).toString()) == 0))
                                                finalDetector = false;
                                            break;
                                        case ">=":
                                            if (!((compareTo(arr[i + 1], hash.get(arr[i]).toString()) > 0) || compareTo(arr[i + 1], hash.get(arr[i]).toString()) == 0))
                                                finalDetector = false;
                                            break;
                                        case ">":
                                            if (!(compareTo(arr[i + 1], hash.get(arr[i]).toString()) > 0))
                                                finalDetector = false;
                                            break;
                                        case "!=":
                                            if (compareTo(arr[i + 1], hash.get(arr[i]).toString()) == 0)
                                                finalDetector = false;
                                            break;
                                    }
                                }
                            }
                            if (arrayOperators[0].equals("OR")) {
                                if (hash.containsKey(arr[i])) {
                                    switch (operatorHash.get(arr[i])) {
                                        case "=":
                                            finalDetector = compareTo(arr[i + 1], hash.get(arr[i]).toString()) == 0;
                                            break;
                                        case "<":
                                            finalDetector = compareTo(arr[i + 1], hash.get(arr[i]).toString()) < 0;
                                            break;
                                        case "<=":
                                            finalDetector = (compareTo(arr[i + 1], hash.get(arr[i]).toString()) < 0) || compareTo(arr[i + 1], hash.get(arr[i]).toString()) == 0;
                                            break;
                                        case ">=":
                                            finalDetector = (compareTo(arr[i + 1], hash.get(arr[i]).toString()) > 0) || compareTo(arr[i + 1], hash.get(arr[i]).toString()) == 0;
                                            break;
                                        case ">":
                                            finalDetector = compareTo(arr[i + 1], hash.get(arr[i]).toString()) > 0;
                                            break;
                                        case "!=":
                                            finalDetector = compareTo(arr[i + 1], hash.get(arr[i]).toString()) != 0;
                                            break;
                                    }
                                    if (finalDetector) break;
                                }
                            }
                            if (arrayOperators[0].equals("XOR")) {
                                if (hash.containsKey(arr[i])) {
                                    switch (operatorHash.get(arr[i])) {
                                        case "=":
                                            if ((compareTo(arr[i + 1], hash.get(arr[i]).toString()) == 0))
                                                finalDetector = false;
                                            break;
                                        case "<":
                                            if ((compareTo(arr[i + 1], hash.get(arr[i]).toString()) < 0))
                                                finalDetector = false;
                                            break;
                                        case "<=":
                                            if (((compareTo(arr[i + 1], hash.get(arr[i]).toString()) < 0) || compareTo(arr[i + 1], hash.get(arr[i]).toString()) == 0))
                                                finalDetector = false;
                                            break;
                                        case ">=":
                                            if (((compareTo(arr[i + 1], hash.get(arr[i]).toString()) > 0) || compareTo(arr[i + 1], hash.get(arr[i]).toString()) == 0))
                                                finalDetector = false;
                                            break;
                                        case ">":
                                            if ((compareTo(arr[i + 1], hash.get(arr[i]).toString()) > 0))
                                                finalDetector = false;
                                            break;
                                        case "!=":
                                            if (compareTo(arr[i + 1], hash.get(arr[i]).toString()) != 0)
                                                finalDetector = false;
                                            break;
                                    }
                                }
                            }
                        }
                        if (finalDetector && !trulyFinalResult.contains(strFinal)) trulyFinalResult.add(strFinal);
                    }
                }
            }
            if(!trulyFinalResult.isEmpty()) iteratorReturned = trulyFinalResult.iterator();
        }
        else iteratorReturned = selectWithoutIndex(sqlTerms,arrayOperators);
        if (iteratorReturned != null) return iteratorReturned;
        else return Collections.emptyIterator();
        }

    public void CombinationRepetition(int arr[], int n, int r) {
        // Allocate memory
        Vector<Integer> temp = new Vector<>();
        int[] chosen = new int[r + 1];

        // Call the recursice function
        CombinationRepetitionUtil(chosen, arr, 0, r, 0, n - 1, temp);
    }

    public void CombinationRepetitionUtil(int[] chosen, int[] arr, int index, int r, int start, int end,
            Vector<Integer> temp) {
        // Since index has become r, current combination is
        // ready to be printed, print
        // boolean flag = true;
        if (index == r) {
            globalVectorOfCombinations.add(new int[r]);
            for (int i = 0; i < r; i++) {
                // System.out.printf("%d ", arr[chosen[i]]);
                globalVectorOfCombinations.elementAt(globalVectorOfCombinations.size() - 1)[i] = arr[chosen[i]];
            }
            // System.out.println(Arrays.toString(globalVectorOfCombinations.elementAt(globalVectorOfCombinations.size()
            // - 1)));
            // System.out.println(globalVectorOfCombinations + "\n");
            // System.out.print("\n");
            return;
        }

        // One by one choose all elements (without considering
        // the fact whether element is already chosen or not)
        // and recur
        for (int i = 0; i <= end; i++) {
            // if (flag) {
            chosen[index] = i;
            // flag = false;
            CombinationRepetitionUtil(chosen, arr, index + 1, r, i, end, temp);
            // }
            // else {
            // chosen[index] = 0;
            // CombinationRepetitionUtil(chosen, arr, index + 1,
            // r, 0, end, temp);
            // }
        }
    }

    public void gridIndexTraversal(Object[] a, int counter, String colName, String tableName, Object value,
            int sizeHelper) {
        this.counterTemp = counter;
        gridIndexTraversalHelper(a, counter, colName, tableName, value, sizeHelper);
    }

    public void gridIndexTraversalHelper(Object[] a, int counter, String colName, String tableName, Object value,
            int sizeHelper) {
        // Vector<Integer> temp = new Vector<>();
        if (counter == 0) {
            String[] colNameArr = new String[] { colName };
            Vector<Object[]> result = gridIndex.getIndicesRange(tableName, colNameArr);
            Object[] extractedArrayOfRanges = result.elementAt(0);
            for (int i = 0; i < extractedArrayOfRanges.length; i++) {
                if (value instanceof Integer) {
                    if (compareTo(Double.valueOf(String.valueOf(value)), extractedArrayOfRanges[i]) <= 0) {
                        rangeVector.add(i);
                        break;
                        // temp.add(this.counterTemp);
                    }

                } else if (compareTo(value, extractedArrayOfRanges[i]) <= 0) {
                    rangeVector.add(i);
                    // temp.add(this.counterTemp);
                    break;
                }
            }
        }
        if (counter > 0) {
            gridIndexTraversal((Object[]) a[0], --counter, colName, tableName, value, sizeHelper);
        }
    }

    public Vector<Object> finalResultHelper(Vector<Object> temp, Vector<Object> finalResult) {
        for (int j = 0; j < temp.size(); j++) {
            finalResult.add(temp.elementAt(j));
        }
        return finalResult;
    }
    // else throw new DBAppException();
    // if (grid.exists()) {
    // gridVector = (Vector<Object[]>) deserializePage("gridIndex.ser");
    // for (int i = 0; i < sqlTerms.length; i++) {
    // columnName = sqlTerms[i].columnName;
    // operator = sqlTerms[i].operator;
    // value = sqlTerms[i].value;
    // }
    // }
    // else throw new DBAppException();
    //
    // return null;
    // }

    public static Bucket getBucket(int[] index, Object[] multiDimArr) throws DBAppException {
        if (index.length == 1) {
            return (Bucket) multiDimArr[index[0]];

        } else {
            int[] newIndex = new int[index.length - 1];
            System.arraycopy(index, 1, newIndex, 0, index.length - 1);
            return getBucket(newIndex, (Object[]) multiDimArr[index[0]]);
        }
    }

    public Vector<Object> retrieveDataFromBucket(Bucket b) throws DBAppException {
        Vector<Object> deserializedPage = new Vector<>();
        String fileName;
        File pageInBucket;
        Vector<Object> tuplesReturned = new Vector<>();
        for (int i = 0; i < b.bucketIndexPage.size(); i++) {
            fileName = b.bucketIndexPage.elementAt(i).toString();
            pageInBucket = new File(System.getProperty("user.dir") + "/src/main/resources/Data/" + fileName);
            if (pageInBucket.exists()) {
                deserializedPage = deserializePage(fileName);
                tuplesReturned.add(deserializedPage.elementAt((int) b.bucketIndexInPage.elementAt(i)));
            } else
                throw new DBAppException();
        }
        return tuplesReturned;
    }

    public Iterator selectWithoutIndex(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        Vector<Vector<Object>> selected = new Vector<>();
        if (arrayOperators[0].toLowerCase().equals("xor")) {
            for (int o = 0; o < sqlTerms.length; o++) {
                if (sqlTerms[o]._strOperator.equals("="))
                    sqlTerms[o]._strOperator = "!=";
                else if (sqlTerms[o]._strOperator.equals("!="))
                    sqlTerms[o]._strOperator = "=";
                else if (sqlTerms[o]._strOperator.equals(">"))
                    sqlTerms[o]._strOperator = "<=";
                else if (sqlTerms[o]._strOperator.equals(">="))
                    sqlTerms[o]._strOperator = "<";
                else if (sqlTerms[o]._strOperator.equals("<"))
                    sqlTerms[o]._strOperator = ">=";
                else if (sqlTerms[o]._strOperator.equals("<="))
                    sqlTerms[o]._strOperator = ">";
            }
            arrayOperators[0] = "AND";
        }
        Hashtable<String, Object> columnNameValue = new Hashtable<>();
        String[] sqlOperators = new String[sqlTerms.length];
        String[] sqlKeys = new String[sqlTerms.length];
        Object[] sqlValues = new Object[sqlTerms.length];
        for (int o = 0; o < sqlTerms.length; o++) {
            sqlKeys[o] = sqlTerms[o]._strColumnName;
            sqlValues[o] = sqlTerms[o]._objValue;
            sqlOperators[o] = sqlTerms[o]._strOperator;
        }
        for (int o = 0; o < sqlTerms.length; o++) {
        }
        String tableName = sqlTerms[0]._strTableName;
        try {
            metaDataFileDetector();
            Set<String> keySetSet = columnNameValue.keySet();
            Vector<String> keySet = new Vector<String>();
            Object[] keySetarr = keySetSet.toArray();
            for (int i = 0; i < keySetarr.length; i++)
                keySet.add((String) keySetarr[i]);
            boolean f = false;
            for (int i = 0; i < keySet.size(); i++) {
                if (!sortHelper(tableName, keySet.get(i))) {
                    f = true;
                    throw new DBAppException();
                }
            }

            boolean f1 = checkHashTable(tableName, columnNameValue);

            if (f == false && f1 == true) {
                int i = 1;
                int j = 1;
                String s = tableName + i + ".ser";
                String pathS = DataPath + "/" + s;
                File file = new File(pathS);
                if (file.exists()) {
                    while (file.exists()) {
                        selected.add(selectWithoutIndexHelper(tableName, columnNameValue, s, sqlOperators, sqlKeys,
                                sqlValues, arrayOperators[0]));
                        j = 1;
                        s = tableName + i + "overflow" + j + ".ser";
                        pathS = DataPath + "/" + s;
                        file = new File(pathS);
                        while (file.exists()) {
                            selected.add(selectWithoutIndexHelper(tableName, columnNameValue, s, sqlOperators, sqlKeys,
                                    sqlValues, arrayOperators[0]));
                            j++;
                            s = tableName + i + "overflow" + j + ".ser";
                            pathS = DataPath + "/" + s;
                            file = new File(pathS);
                        }
                        i++;
                        s = tableName + i + ".ser";
                        pathS = DataPath + "/" + s;
                        file = new File(pathS);

                    }
                } else
                    System.out.println("Table name Is Not Correct");
            }
        } catch (Exception e) {
            throw new DBAppException();
        }
        Vector<Object> finalSelected = new Vector<>();
        for (int i = 0; i < selected.size(); i++) {
            Vector<Object> v = selected.get(i);
            for (int j = 0; j < v.size(); j++)
                finalSelected.add(v.get(j));
        }
        if (finalSelected.size() == 0)
            return Collections.emptyIterator();
        else
            return finalSelected.iterator();
    }

    public Vector<Object> selectWithoutIndexHelper(String tableName, Hashtable<String, Object> columnNameValue,
            String s, String[] sqlOperators, String[] keys, Object[] values, String operator)
            throws DBAppException, IOException {
        Vector<Object> selected = new Vector<>();
        Vector<Object> deserializePage = deserializePage(s);
        boolean Select = false;

        if (operator.toLowerCase().equals("or")) {
            for (int i = 0; i < deserializePage.size(); i++) {
                String tuple = (deserializePage.get(i)).toString();
                String[] tupleSplite = tuple.split(",");
                for (int z = 0; z < tupleSplite.length; z++)
                    tupleSplite[z] = tupleSplite[z].trim();
                for (int j = 0; j < keys.length; j++) {
                    for (int z = 1; z < tupleSplite.length; z++) {
                        if (tupleSplite[z].equals(keys[j])) {
                            if (sqlOperators[j].equals("=")) {
                                if (tupleSplite[z + 1].equals(String.valueOf(values[j]))) {
                                    selected.add(deserializePage.get(i));
                                    Select = true;
                                    break;
                                }
                            } else if (sqlOperators[j].equals("!=")) {
                                if (!(tupleSplite[z + 1].equals(String.valueOf(values[j])))) {
                                    selected.add(deserializePage.get(i));
                                    Select = true;
                                    break;
                                }
                            } else if (sqlOperators[j].equals(">")) {
                                if (tupleSplite[z + 1].compareTo(String.valueOf(values[j])) > 0) {
                                    selected.add(deserializePage.get(i));
                                    Select = true;
                                    break;
                                }
                            } else if (sqlOperators[j].equals(">=")) {
                                if ((tupleSplite[z + 1].equals(String.valueOf(values[j])))
                                        || (tupleSplite[z + 1].compareTo(String.valueOf(values[j])) > 0)) {
                                    selected.add(deserializePage.get(i));
                                    Select = true;
                                    break;
                                }
                            } else if (sqlOperators[j].equals("<")) {
                                if (tupleSplite[z + 1].compareTo(String.valueOf(values[j])) < 0) {
                                    selected.add(deserializePage.get(i));
                                    Select = true;
                                    break;
                                }
                            } else if (sqlOperators[j].equals("<=")) {
                                if ((tupleSplite[z + 1].equals(String.valueOf(values[j])))
                                        || (tupleSplite[z + 1].compareTo(String.valueOf(values[j])) < 0)) {
                                    selected.add(deserializePage.get(i));
                                    Select = true;
                                    break;
                                }
                            }
                        } else
                            z++;
                    }
                    if (Select == true)
                        break;
                }
                Select = false;
            }
        } else if (operator.toLowerCase().equals("and")) {
            for (int i = 0; i < deserializePage.size(); i++) {
                String tuple = (deserializePage.get(i)).toString();
                String[] tupleSplite = tuple.split(",");
                for (int z = 0; z < tupleSplite.length; z++)
                    tupleSplite[z] = tupleSplite[z].trim();
                for (int j = 0; j < keys.length; j++) {
                    for (int z = 1; z < tupleSplite.length; z++) {
                        if (tupleSplite[z].equals(keys[j])) {
                            if (sqlOperators[j].equals("=")) {
                                if (tupleSplite[z + 1].equals(String.valueOf(values[j]))) {
                                    Select = true;
                                    break;
                                } else {
                                    Select = false;
                                    break;
                                }
                            } else if (sqlOperators[j].equals("!=")) {
                                if (!(tupleSplite[z + 1].equals(String.valueOf(values[j])))) {
                                    Select = true;
                                    break;
                                } else {
                                    Select = false;
                                    break;
                                }
                            } else if (sqlOperators[j].equals(">")) {
                                if (tupleSplite[z + 1].compareTo(String.valueOf(values[j])) > 0) {
                                    Select = true;
                                    break;
                                } else {
                                    Select = false;
                                    break;
                                }
                            } else if (sqlOperators[j].equals(">=")) {
                                if ((tupleSplite[z + 1].equals(String.valueOf(values[j])))
                                        || (tupleSplite[z + 1].compareTo(String.valueOf(values[j])) > 0)) {
                                    Select = true;
                                    break;
                                } else {
                                    Select = false;
                                    break;
                                }
                            } else if (sqlOperators[j].equals("<")) {
                                if (tupleSplite[z + 1].compareTo(String.valueOf(values[j])) < 0) {
                                    Select = true;
                                    break;
                                } else {
                                    Select = false;
                                    break;
                                }
                            } else if (sqlOperators[j].equals("<=")) {
                                if ((tupleSplite[z + 1].equals(String.valueOf(values[j])))
                                        || (tupleSplite[z + 1].compareTo(String.valueOf(values[j])) < 0)) {
                                    Select = true;
                                    break;
                                } else {
                                    Select = false;
                                    break;
                                }
                            }
                        } else
                            z++;
                    }
                }
                if (Select == true)
                    selected.add(deserializePage.get(i));
                Select = false;
            }
        }
        return selected;
    }

    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        try {
            metaDataFileDetector();
            Set<String> keySetSet = columnNameValue.keySet();
            Vector<String> keySet = new Vector<String>();
            Object[] keySetarr = keySetSet.toArray();
            for (int i = 0; i < keySetarr.length; i++)
                keySet.add((String) keySetarr[i]);
            boolean f = false;
            for (int i = 0; i < keySet.size(); i++) {
                if (!sortHelper(tableName, keySet.get(i))) {
                    f = true;
                    throw new DBAppException();
                }
            }

            boolean f1 = checkHashTable(tableName, columnNameValue);

            if (f == false && f1 == true) {
                int i = 1;
                int j = 1;
                String s = tableName + i + ".ser";
                String pathS = DataPath + "/" + s;
                File file = new File(pathS);
                if (file.exists()) {
                    while (file.exists()) {
                        deleteFromTableHelper(tableName, columnNameValue, s, i, 0);
                        j = 1;
                        s = tableName + i + "overflow" + j + ".ser";
                        pathS = DataPath + "/" + s;
                        file = new File(pathS);
                        while (file.exists()) {
                            deleteFromTableHelper(tableName, columnNameValue, s, i, j);
                            j++;
                            s = tableName + i + "overflow" + j + ".ser";
                            pathS = DataPath + "/" + s;
                            file = new File(pathS);
                        }
                        i++;
                        s = tableName + i + ".ser";
                        pathS = DataPath + "/" + s;
                        file = new File(pathS);

                    }
                } else
                    System.out.println("Table name Is Not Correct");
            }
        } catch (Exception e) {
            throw new DBAppException();
        }
    }

    public static void deleteTableFromMetaData(String tableName) {
        String metaDataPath = System.getProperty("user.dir") + "/src/main/resources/" + "metadata.csv";
        Scanner sc;
        String data = "";
        try {
            sc = new Scanner(new File(metaDataPath));
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                String[] lineSplite = line.split(",");
                if (!((lineSplite[0].trim()).equals(tableName)))
                    data = data + line + "\n";
            }
            sc.close();
            FileWriter metaData = new FileWriter(new File(metaDataPath));
            metaData.write(data);
            metaData.close();
        } catch (Exception e) {
            System.out.println("cannot delete table. table doesnt exist.");
        }

    }

    public void renameFiles(String tableName, int pageNumber, int overFlow) {
        int i = pageNumber + 1;
        int j = 1;
        String testOverflow;
        File fileTestOverflow;
        boolean stopRenaming = false;
        Vector<Object> deserializePage = new Vector<Object>();
        String s;
        File file;
        String pathS;

        if (overFlow == 0) {
            s = tableName + (i - 1) + "overflow" + j + ".ser";

            pathS = DataPath + "/" + s;
            file = new File(pathS);
            if (file.exists()) {
                while (file.exists()) {
                    testOverflow = DataPath + "/" + tableName + i + ".ser";
                    fileTestOverflow = new File(testOverflow);
                    if (fileTestOverflow.exists()) {
                        deserializePage = deserializePage(s);
                        deleteFile(s);
                        serializeOverflow(tableName, i, j - 1, deserializePage);
                        j++;
                        s = tableName + i + "overflow" + j + ".ser";
                        pathS = DataPath + "/" + s;
                        file = new File(pathS);
                    } else {
                        deserializePage = deserializePage(s);
                        deleteFile(s);
                        serializePage(tableName, i, deserializePage);
                        j++;
                        s = tableName + i + "overflow" + j + ".ser";
                        pathS = DataPath + "/" + s;
                        file = new File(pathS);
                    }
                }
            } else {
                s = tableName + i + ".ser";
                pathS = DataPath + "/" + s;
                file = new File(pathS);
                while (file.exists()) {
                    deserializePage = deserializePage(s);
                    deleteFile(s);
                    serializePage(tableName, i - 1, deserializePage);
                    j = 1;
                    s = tableName + i + "overflow" + j + ".ser";
                    pathS = DataPath + "/" + s;
                    file = new File(pathS);
                    while (file.exists()) {
                        testOverflow = DataPath + "/" + tableName + i + ".ser";
                        fileTestOverflow = new File(testOverflow);
                        if (fileTestOverflow.exists()) {
                            deserializePage = deserializePage(s);
                            deleteFile(s);
                            serializeOverflow(tableName, i, j - 1, deserializePage);
                            j++;
                            s = tableName + i + "overflow" + j + ".ser";
                            pathS = DataPath + "/" + s;
                            file = new File(pathS);
                            stopRenaming = true;
                        } else {
                            deserializePage = deserializePage(s);
                            deleteFile(s);
                            serializePage(tableName, i, deserializePage);
                            j++;
                            s = tableName + i + "overflow" + j + ".ser";
                            pathS = DataPath + "/" + s;
                            file = new File(pathS);
                            stopRenaming = true;
                        }
                    }
                    if (stopRenaming == true) {
                        break;
                    }
                    i++;
                    s = tableName + i + ".ser";
                    pathS = DataPath + "/" + s;
                    file = new File(pathS);
                }
            }
        } else {
            int z = overFlow + 1;
            s = tableName + (i - 1) + "overflow" + z + ".ser";
            pathS = DataPath + "/" + s;
            file = new File(pathS);
            while (file.exists()) {
                deserializePage = deserializePage(s);
                deleteFile(s);
                serializeOverflow(tableName, pageNumber, z - 1, deserializePage);
                z++;
                s = tableName + pageNumber + "overflow" + z + ".ser";
                pathS = DataPath + "/" + s;
                file = new File(pathS);
            }
        }

    }

    public void deleteFromTableHelper(String tableName, Hashtable<String, Object> columnNameValue, String s,
            int pageNumber, int overFlow) throws DBAppException, IOException {
        Vector<Object> deserializePage = deserializePage(s);
        Vector<Object> newPage = new Vector<Object>();
        Set<String> keySetSet = columnNameValue.keySet();
        Vector<String> keySet = new Vector<String>();
        Object[] keySetarr = keySetSet.toArray();
        Object[] htValues = new Object[(keySetarr.length)];
        boolean delete = false;

        for (int i = 0; i < keySetarr.length; i++) {
            keySet.add((String) keySetarr[i]);
            htValues[i] = columnNameValue.get(keySet.get(i));
        }
        for (int i = 0; i < deserializePage.size(); i++) {
            String tuple = (deserializePage.get(i)).toString();
            String[] tupleSplite = tuple.split(",");
            for (int z = 0; z < tupleSplite.length; z++)
                tupleSplite[z] = tupleSplite[z].trim();
            for (int j = 0; j < keySet.size(); j++) {
                for (int z = 1; z < tupleSplite.length; z++) {
                    if (tupleSplite[z].equals(keySet.get(j))) {
                        if (tupleSplite[z + 1].equals(String.valueOf(htValues[j]))) {
                            delete = true;
                            break;
                        } else {
                            delete = false;
                            newPage.add(deserializePage.get(i));
                            break;
                        }
                    } else
                        z++;
                }
                if (!delete)
                    break;
            }
            if (delete == true) {
                String primaryK = readClusteringKeyFromeMetadata(tableName).get(1);
                String primaryKToDelete = "";
                for (int z = 1; z < tupleSplite.length; z++) {
                    if (tupleSplite[z].equals(primaryK)) {
                        primaryKToDelete = tupleSplite[z + 1];
                        break;
                    } else
                        z++;
                }
                if (overFlow == 0) {
                    String k = tableName + "0.ser";
                    File file = new File(DataPath + "/" + k);
                    if (file.exists()) {
                        Vector<Object> h = deserializePage(k);
                        if (h.get(0).getClass().getName().equals("java.lang.Integer"))
                            h.removeElement(Integer.valueOf(primaryKToDelete));
                        else if (h.get(0).getClass().getName().equals("java.lang.String"))
                            h.removeElement(String.valueOf(primaryKToDelete));
                        else if (h.get(0).getClass().getName().equals("java.lang.Double"))
                            h.removeElement(Double.valueOf(primaryKToDelete));
                        else {
                            Vector temp = new Vector<>();
                            for (int z = 0; z < h.size(); z++) {
                                if (!String.valueOf(h.get(z)).equals(primaryKToDelete))
                                    temp.add(h.get(z));
                            }
                            h = temp;
                        }
                        if (h.size() == 0)
                            deleteFile(k);
                        else
                            serializePage(tableName, 0, h);
                    } else {
                        String k1 = tableName + "0overflow0.ser";
                        File file1 = new File(DataPath + "/" + k1);
                        if (file1.exists()) {
                            Vector<Object> h = deserializePage(k1);
                            if (h.get(0).getClass().getName().equals("java.lang.Integer"))
                                h.removeElement(Integer.valueOf(primaryKToDelete));
                            else if (h.get(0).getClass().getName().equals("java.lang.String"))
                                h.removeElement(String.valueOf(primaryKToDelete));
                            else if (h.get(0).getClass().getName().equals("java.lang.Double"))
                                h.removeElement(Double.valueOf(primaryKToDelete));
                            else {
                                Vector temp = new Vector<>();
                                for (int z = 0; z < h.size(); z++) {
                                    if (!String.valueOf(h.get(z)).equals(primaryKToDelete))
                                        temp.add(h.get(z));
                                }
                                h = temp;
                            }
                            if (h.size() == 0)
                                deleteFile(k1);
                            else
                                serializePage(tableName, 0, h);

                        }
                    }
                }
            }
            delete = false;
        }

        if (newPage.size() == 0) {
            deleteFile(s);
            renameFiles(tableName, pageNumber, overFlow);
        } else {
            if (overFlow == 0) {
                serializePage(tableName, pageNumber, newPage);
            } else {
                serializeOverflow(tableName, pageNumber, overFlow, newPage);
            }
        }

    }

    public static void deleteFile(String s) {
        String s1 = (s.split("\\.")[0]);
        String isZero = s1.substring(s1.length() - 1);
        if (isZero.equals("0"))
            deleteTableFromMetaData(s1.substring(0, s1.length() - 1));
        String path1 = DataPath + "/" + s;
        Path path = FileSystems.getDefault().getPath(path1);
        try {
            Files.deleteIfExists(path);
        } catch (IOException x) {
            System.out.println("error cant delete file");
        }
    }

    public static boolean checkHashTable(String tableName, Hashtable<String, Object> columnNameValue)
            throws ParseException {
        String metaDataPath = System.getProperty("user.dir") + "/src/main/resources" + "/Metadata.csv";
        Vector<String> columnNames = new Vector<String>();
        Vector<String> columnType = new Vector<String>();
        Set<String> keySetSet = columnNameValue.keySet();
        Vector<String> keySet = new Vector<String>();
        Object[] keySetarr = keySetSet.toArray();
        Object[] htValues = new Object[(keySetarr.length)];
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        for (int i = 0; i < keySetarr.length; i++) {
            keySet.add((String) keySetarr[i]);
            htValues[i] = columnNameValue.get(keySet.get(i));
        }
        int j = 0;

        File file = new File(metaDataPath);
        if (file.exists()) {
            try (BufferedReader br = new BufferedReader(new FileReader(metaDataPath))) {
                String line = br.readLine();
                while (line != null) {
                    String[] s = line.split(",");
                    if ((s[0].trim()).equals(tableName)) {
                        columnNames.add(s[1].trim());
                        columnType.add(s[2].trim());
                    }
                    line = br.readLine();
                }
            } catch (IOException e) {
                System.out.println("check meta-data file");
                return false;
            }
            int z = 0;
            for (int i = 0; i < keySetarr.length; i++) {
                for (z = 0; z < columnNames.size(); z++) {
                    if (keySetarr[i].toString().trim().equals(columnNames.get(z).trim()))
                        break;
                }
                if (!(keySetarr[i].toString().trim().equals(columnNames.get(z).trim()))) {
                    return false;
                }

            }
            for (int i = 0; i < keySetarr.length; i++) {
                for (j = 0; j < columnNames.size(); j++) {
                    if (keySetarr[i].toString().trim().equals((columnNames.get(j)).trim())) {
                        if ((columnType.get(i)).trim().equals("java.lang.Integer")
                                && !(htValues[i] instanceof Integer)) {
                            return false;
                        }
                        if ((columnType.get(i)).trim().equals("java.lang.Double") && !(htValues[i] instanceof Double)) {
                            return false;
                        }
                        if ((columnType.get(i)).trim().equals("java.lang.String") && !(htValues[i] instanceof String)) {
                            System.out.println(htValues[i]);
                            return false;
                        }
                        if (((columnType.get(i)).trim().equals("java.lang.Date"))
                                && !((sdf.parse((htValues[i].toString()).trim()) instanceof Date))) {
                            return false;
                        }
                        break;
                    }
                }
                if (!(keySetarr[i].toString().trim().equals((columnNames.get(j)).trim())))
                    return false;
            }
            return true;
        }
        return false;
    }

    public static void main(String[] args) throws DBAppException, IOException, ParseException {

        String strTableName = "Student";
        DBApp dbApp = new DBApp();
        Hashtable htblColNameType = new Hashtable();
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.double");
        Hashtable min = new Hashtable();
        min.put("id", "0");
        min.put("name", "a");
        min.put("gpa", "0.0");
        Hashtable max = new Hashtable();
        max.put("id", "10");
        max.put("name", "zzzz");
        max.put("gpa", "10.0");
        dbApp.init();
        dbApp.createTable(strTableName, "id", htblColNameType, min, max);

        Hashtable htblColNameValue = new Hashtable();
        htblColNameValue.put("id", new Integer(0));
        htblColNameValue.put("name", new String("Ahmed Noor"));
        htblColNameValue.put("gpa", new Double(1));
        dbApp.insertIntoTable(strTableName, htblColNameValue);
        htblColNameValue.clear();
        htblColNameValue.put("id", new Integer(1));
        htblColNameValue.put("name", new String("Ahmed Noor"));
        htblColNameValue.put("gpa", new Double(2));
        dbApp.insertIntoTable(strTableName, htblColNameValue);
        htblColNameValue.clear();
        htblColNameValue.put("id", new Integer(2));
        htblColNameValue.put("name", new String("Dalia Noor"));
        htblColNameValue.put("gpa", new Double(3));
        dbApp.insertIntoTable(strTableName, htblColNameValue);
        htblColNameValue.clear();
        htblColNameValue.put("id", new Integer(3));
        htblColNameValue.put("name", new String("John Noor"));
        htblColNameValue.put("gpa", new Double(4));
        dbApp.insertIntoTable(strTableName, htblColNameValue);
        htblColNameValue.clear();
        htblColNameValue.put("id", new Integer(4));
        htblColNameValue.put("name", new String("Zaky Noor"));
        htblColNameValue.put("gpa", new Double(5));
        dbApp.insertIntoTable(strTableName, htblColNameValue);

        SQLTerm sql = new SQLTerm();
        sql._strTableName = "Student";
        sql._strColumnName = "id";
        sql._objValue = 2;
        sql._strOperator = "=";
        SQLTerm sql1 = new SQLTerm();
        sql1._strTableName = "Student";
        sql1._strColumnName = "id";
        sql1._objValue = 3;
        sql1._strOperator = "=";
        SQLTerm[] s = new SQLTerm[] { sql, sql1 };
        String[] arr = new String[] { "xor" };
        Vector page = gridIndex.deserialize("Student1");
        for (int i = 0; i < page.size(); i++) {
            System.out.println(page.get(i));
        }
        dbApp.selectFromTable(s, arr);
        page = gridIndex.deserialize("Student1");
        for (int i = 0; i < page.size(); i++) {
            System.out.println(page.get(i));
        }

        // dbApp.createIndex(strTableName, new String[] { "gpa" });

        // Vector v = gridIndex.deserialize("gpa-id");
        // Object[] grid = (Object[]) v.get(0);
        // for (int y = 0; y < 10; y++) {
        // for (int z = 0; z < 10; z++) {
        // Bucket b = (Bucket) ((Object[]) grid[y])[z];
        // for (int i = 0; i < b.getBucketSize(); i++)
        // System.out.println(b.getIndexInPage(i) + " " + y + " " + z);

        // }
        // }

        // Vector page = gridIndex.deserialize("Student1");
        // for (int i = 0; i < page.size(); i++) {
        // System.out.println(page.get(i));
        // }

        // DBApp dbApp = new DBApp();
        // Hashtable columnNameValue = new Hashtable();
        // // columnNameValue.put("id", 0);
        // columnNameValue.put("id", "2");
        // //columnNameValue.put("gpa", 1.0);
        // dbApp.deleteFromTable("Student", columnNameValue);
        // // dbApp.updateTable("Student", "4", columnNameValue);
        // page = gridIndex.deserialize("Student1");
        // for (int i = 0; i < page.size(); i++)
        // System.out.println(page.get(i));

        // Vector keys = gridIndex.deserialize("Student0");
        // for (int i = 0; i < keys.size(); i++) {
        // System.out.print(keys.get(i));
        // }
        // Vector<String> v = new Vector<>();
        // v.add("2000/06/01");
        // v.add("2000/06/02");
        // v.add("2000/06/03");
        // System.out.println(binarySearchString(v,"2000/06/01"));

    }

}
