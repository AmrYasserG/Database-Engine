package DBproject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Scanner;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

public class gridIndex implements Serializable {
    static String DataPath = System.getProperty("user.dir") + "/src/main/resources/Data/";
    Object[] multiDimArr;

    public gridIndex(String[] colName, String tableName, Vector<Vector> pages, Vector<String> pages2)
            throws DBAppException {
        int colNumber = 0;
        for (int i = 0; i < colName.length; i++)
            colNumber++;
        multiDimArr = multiDimArr(colNumber);
        multiDimArr = fillWithBuckets(multiDimArr);
        Vector<Object[]> indicesRange = getIndicesRange(tableName, colName);
        for (int i = 0; i < pages.size(); i++)
            determineIndex(pages.get(i), multiDimArr, indicesRange, colName, tableName, pages2.get(i));
        String gridName = "";
        for (int i = 0; i < colName.length; i++) {
            if (i == colName.length - 1)
                gridName = gridName + colName[i];
            else
                gridName = gridName + colName[i] + "-";
        }

        File file = new File(DataPath + "gridIndices" + ".ser");
        Vector<String> gridIndices = new Vector<String>();
        if (file.exists())
            gridIndices = deserialize("gridIndices");
        gridIndices.add(gridName);
        serialize("gridIndices", gridIndices);
        Vector<Object> NDIndexVector = new Vector<>();
        NDIndexVector.add(multiDimArr);
        serialize(gridName, NDIndexVector);
        for (int i = 0; i < colName.length; i++)
            changeIndexedMetaData(tableName, colName[i]);
    }

    public static void changeIndexedMetaData(String tableName, String colName) {
        String metaDataPath = System.getProperty("user.dir") + "/src/main/resources/" + "metadata.csv";
        Scanner sc;
        String data = "";
        try {
            sc = new Scanner(new File(metaDataPath));
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                String[] lineSplite = line.split(",");
                if (((lineSplite[0].trim()).equals(tableName)) && (lineSplite[1].equals(colName))) {
                    lineSplite[4] = "true";
                    String s = "";
                    for (int i = 0; i < lineSplite.length; i++)
                        s = s + lineSplite[i] + ",";
                    data = data + s + "\n";
                } else
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

    public Object[] getMultiDimArr() {
        return multiDimArr;
    }

    public static void determineIndex(Vector page, Object[] multiDimArr, Vector<Object[]> indicesRange,
            String[] colName, String tableName, String page2) throws DBAppException {

        for (int i = 0; i < page.size(); i++) {
            Vector<Integer> index = new Vector<Integer>();
            String tuple = (page.get(i)).toString();
            String[] tupleSplite = tuple.split(",");
            for (int j = 0; j < tupleSplite.length; j++)
                tupleSplite[j] = tupleSplite[j].trim();
            Vector<Integer> ColNameIndex = new Vector<Integer>();
            for (int j = 0; j < colName.length; j++)
                ColNameIndex.add(getColNameIndex(tableName, colName[j]));
            for (int j = 0; j < ColNameIndex.size(); j++) {
                Object[] range = indicesRange.get(j);
                try {
                    Double[] rangeDou = new Double[range.length];
                    for (int q = 0; q < range.length; q++)
                        rangeDou[q] = (Double) range[q];
                    for (int y = 0; y < rangeDou.length; y++) {
                        if (Double.valueOf(tupleSplite[ColNameIndex.get(j)]) <= rangeDou[y]) {
                            index.add(y);
                            break;
                        }
                    }

                } catch (Exception e) {
                    try {
                        Integer[] rangeInt = new Integer[range.length];
                        for (int q = 0; q < range.length; q++)
                            rangeInt[q] = (Integer) range[q];
                        for (int y = 0; y < rangeInt.length; y++) {
                            if (Integer.valueOf(tupleSplite[ColNameIndex.get(j)]) <= rangeInt[y]) {
                                index.add(y);
                                break;
                            }
                        }

                    } catch (Exception a) {
                        try {
                            String[] rangeDate = new String[range.length];
                            for (int q = 0; q < range.length; q++)
                                rangeDate[q] = (String) range[q];
                            for (int y = 0; y < rangeDate.length; y++) {
                                if (compare2Dates(rangeDate[y], tupleSplite[ColNameIndex.get(j)]) <= 0) {
                                    index.add(y);
                                    break;
                                }
                            }

                        } catch (Exception b) {
                            char[] rangeString = new char[range.length];
                            for (int q = 0; q < range.length; q++)
                                rangeString[q] = (char) range[q];
                            for (int y = 0; y < rangeString.length; y++) {
                                if (Character.compare((tupleSplite[ColNameIndex.get(j)]).charAt(0),
                                        rangeString[y]) <= 0) {
                                    index.add(y);
                                    break;
                                }
                            }
                        }
                    }
                }

            }
            int[] indexIntArray = new int[index.size()];
            for (int z = 0; z < index.size(); z++)
                indexIntArray[z] = index.get(z);
            insertIntoBucket(indexIntArray, multiDimArr, page2, i);
        }
    }

    public static void insertIntoBucket(int[] index, Object[] multiDimArr, String page, int indexInPage)
            throws DBAppException {
        if (index.length == 1) {
            Bucket b = ((Bucket) multiDimArr[index[0]]);
            b.add(page, indexInPage);
            multiDimArr[index[0]] = b;
        } else {
            int[] newIndex = new int[index.length - 1];
            for (int i = 0; i < index.length - 1; i++)
                newIndex[i] = index[i + 1];
            insertIntoBucket(newIndex, (Object[]) multiDimArr[index[0]], page, indexInPage);
        }
    }

    public static Object[] multiDimArr(int dim) {
        Object[] baseArr = new Object[10];
        for (int i = 0; i < dim - 1; i++)
            multiDimArrHelper(baseArr);
        return baseArr;
    }

    public static Object[] multiDimArrHelper(Object[] arr) {
        if (arr[0] == null) {
            for (int i = 0; i < 10; i++) {
                Object[] fillArr = new Object[10];
                arr[i] = fillArr;
            }
        } else
            for (int i = 0; i < 10; i++)
                arr[i] = multiDimArrHelper((Object[]) arr[i]);
        return arr;
    }

    public static Object[] fillWithBuckets(Object[] arr) {
        if (arr[0] == null) {
            for (int i = 0; i < 10; i++) {
                Bucket b = new Bucket();
                arr[i] = b;
            }
        } else
            for (int i = 0; i < 10; i++)
                arr[i] = fillWithBuckets((Object[]) arr[i]);
        return arr;
    }

    public static Vector<Object[]> getIndicesRange(String tableName, String[] ColName) {
        Vector<Object[]> result = new Vector<>();
        String max = getColNameVal(tableName, ColName[0], "max");
        String min = getColNameVal(tableName, ColName[0], "min");
        if (!max.equals("-1") && !min.equals("-1")) {
            for (int i = 0; i < ColName.length; i++) {
                max = getColNameVal(tableName, ColName[i], "max");
                min = getColNameVal(tableName, ColName[i], "min");
                try {
                    Double maxDou = Double.valueOf(max);
                    Double minDou = Double.valueOf(min);
                    Double range = maxDou - minDou;
                    Double[] index = new Double[10];
                    for (int j = 0; j < 10; j++)
                        index[j] = (j * (range / 10)) + (range / 10) + minDou;
                    result.add((Object[]) index);

                } catch (Exception e) {
                    try {
                        int maxInt = Integer.valueOf(max);
                        int minInt = Integer.valueOf(min);
                        int range = maxInt - minInt;
                        Integer[] index = new Integer[10];
                        if (range < 10) {
                            int t = minInt;
                            for (int z = 0; z < range; z++) {
                                index[z] = t++;
                                if (z == range - 1)
                                    index[z + 1] = t;
                            }
                        } else {
                            int mod = range % 10;
                            int div = range / 10;
                            int value = minInt;
                            for (int j = 0; j < 10; j++) {
                                if (mod > -1) {
                                    if (j == 0)
                                        value = value + div;
                                    else
                                        value = value + div + 1;
                                    index[j] = value;
                                    mod--;
                                } else {
                                    value = value + div;
                                    index[j] = value;
                                }
                            }
                        }
                        result.add((Object[]) index);

                    } catch (Exception a) {
                        try {
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                            java.util.Date testMin = new java.util.Date(sdf.parse(min).getTime());
                            java.util.Date testMax = new java.util.Date(sdf.parse(max).getTime());
                            int range = compare2Dates(min, max);
                            String[] index = new String[10];
                            if (range < 10) {
                                String date = min;
                                for (int z = 0; z < range; z++) {
                                    index[z] = date;
                                    date = addToDate(date, 1);
                                    if (z == range - 1)
                                        index[z + 1] = date;

                                }
                            } else {
                                int mod = (range % 10);
                                int div = (range / 10);
                                String date = min;
                                for (int j = 0; j < 10; j++) {
                                    if (mod > -1) {
                                        if (j == 0)
                                            date = addToDate(date, div);
                                        else
                                            date = addToDate(date, div + 1);
                                        index[j] = date;
                                        mod--;
                                    } else {
                                        date = addToDate(date, div);
                                        index[j] = date;
                                    }
                                }
                            }
                            result.add((Object[]) index);

                        } catch (Exception b) {
                            Character[] index = new Character[10];
                            char[] alphabets = new char[] { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
                                    'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z' };
                            int alphabetsNum = 0;
                            for (int j = 0; j < alphabets.length; j++) {
                                if ((Character.compare(min.toLowerCase().charAt(0), alphabets[j]) <= 0)
                                        && (Character.compare(alphabets[j], max.toLowerCase().charAt(0)) <= 0))
                                    alphabetsNum++;
                            }
                            if (max.length() > min.length() + 1) {
                                index = new Character[] { 'c', 'f', 'i', 'l', 'o', 'r', 't', 'v', 'x', 'z' };
                            } else if (max.length() == min.length()) {
                                if (alphabetsNum < 10) {
                                    int counter = 0;
                                    for (int j = 0; j < alphabets.length; j++) {
                                        if ((Character.compare(min.toLowerCase().charAt(0), alphabets[j]) <= 0)
                                                && (Character.compare(alphabets[j], max.toLowerCase().charAt(0)) <= 0))
                                            index[counter++] = alphabets[j];
                                    }
                                } else {
                                    char[] range = new char[alphabetsNum];
                                    int counter = 0;
                                    for (int j = 0; j < alphabets.length; j++) {
                                        if ((Character.compare(min.toLowerCase().charAt(0), alphabets[j]) <= 0)
                                                && (Character.compare(alphabets[j], max.toLowerCase().charAt(0)) <= 0))
                                            range[counter++] = alphabets[j];
                                    }
                                    int mod = alphabetsNum % 10;
                                    int div = alphabetsNum / 10;
                                    int value = 0 - 1;
                                    for (int j = 0; j < index.length; j++) {
                                        if (mod > 0) {
                                            value = value + div + 1;
                                            index[j] = range[value];
                                            mod--;
                                        } else {
                                            value = value + div;
                                            index[j] = range[value];
                                        }
                                    }
                                }
                            } else if (max.length() == min.length() + 1) {
                                alphabetsNum = 0;
                                int loop = 1;
                                for (int j = 0; j < alphabets.length; j++) {
                                    if ((Character.compare(min.toLowerCase().charAt(0), alphabets[j]) <= 0)
                                            && loop == 1)
                                        alphabetsNum++;
                                    if (j == alphabets.length - 1 && loop == 1) {
                                        j = 0;
                                        loop++;
                                    }
                                    if ((Character.compare(alphabets[j], max.toLowerCase().charAt(0)) <= 0)
                                            && loop == 2)
                                        alphabetsNum++;
                                }
                                char[] range = new char[alphabetsNum];
                                int counter = 0;
                                loop = 1;
                                for (int j = 0; j < alphabets.length; j++) {
                                    if ((Character.compare(min.toLowerCase().charAt(0), alphabets[j]) <= 0)
                                            && loop == 1)
                                        range[counter++] = alphabets[j];
                                    if (j == alphabets.length - 1 && loop == 1) {
                                        j = 0;
                                        loop++;
                                    }
                                    if ((Character.compare(alphabets[j], max.toLowerCase().charAt(0)) <= 0)
                                            && loop == 2)
                                        range[counter++] = alphabets[j];
                                }
                                Arrays.sort(range);
                                Vector<Character> trueRange = new Vector<>();
                                for (int j = 0; j < range.length; j++) {
                                    if (j != 0 && range[j - 1] == range[j]) {
                                    } else
                                        trueRange.add(range[j]);
                                }
                                alphabetsNum = trueRange.size();
                                range = new char[alphabetsNum];
                                for (int j = 0; j < trueRange.size(); j++)
                                    range[j] = trueRange.get(j);
                                if (alphabetsNum < 10) {
                                    for (int j = 0; j < range.length; j++)
                                        index[j] = range[j];
                                } else {
                                    int mod = alphabetsNum % 10;
                                    int div = alphabetsNum / 10;
                                    int value = 0 - 1;
                                    for (int j = 0; j < index.length; j++) {
                                        if (mod > 0) {
                                            value = value + div + 1;
                                            index[j] = range[value];
                                            mod--;
                                        } else {
                                            value = value + div;
                                            index[j] = range[value];
                                        }
                                    }
                                }
                            }
                            result.add((Object[]) index);
                        }
                    }
                }

            }

            return result;
        } else
            return result;
    }

    public static String getColNameVal(String tableName, String ColName, String attribute) {
        String metaDataPath = System.getProperty("user.dir") + "/src/main/resources/" + "metadata.csv";
        Scanner sc;
        try {
            sc = new Scanner(new File(metaDataPath));
            int attributeIndex = -1;
            String found = "-1";
            while (sc.hasNextLine()) {
                String[] line = sc.nextLine().split(",");
                if (attributeIndex == -1) {
                    for (int i = 0; i < line.length; i++) {
                        if (attribute.equals(line[i])) {
                            attributeIndex = i;
                            break;
                        }
                    }
                }
                if (line[0].equals(tableName) && line[1].equals(ColName)) {
                    found = line[attributeIndex];
                    break;
                }
            }
            sc.close();
            return found;

        } catch (FileNotFoundException e) {
            return "-1";
        }
    }

    public static int getColNameIndex(String tableName, String ColName) {
        int result = -1;
        Vector v = deserialize(tableName + "1");
        String[] line = ((String) v.get(0)).split(",");
        for (int i = 0; i < line.length; i++)
            line[i] = line[i].trim();
        for (int i = 0; i < line.length; i++) {
            if (line[i].equals(ColName)) {
                result = i + 1;
                break;
            }
        }
        return result;
    }

    public static void serialize(String name, Vector Object) {
        try {
            String path = DataPath + name + ".ser";
            FileOutputStream file = new FileOutputStream(path);
            ObjectOutputStream out = new ObjectOutputStream(file);
            out.writeObject(Object);
            out.close();
            file.close();
            System.out.println("Serialization complete");

        } catch (IOException ex) {
            System.out.println("IOException is caught");
        }

    }

    public static Vector deserialize(String name) {
        Vector deserialize = new Vector<>();
        try {
            String path = DataPath + name + ".ser";
            FileInputStream fileIn = new FileInputStream(path);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            deserialize = (Vector) in.readObject();
            in.close();
            fileIn.close();

        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
        }
        return deserialize;
    }

    public static String addToDate(String oldDate, int number) {
        String result = "";
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Calendar c = Calendar.getInstance();
            c.setTime(sdf.parse(oldDate));
            c.add(Calendar.DAY_OF_MONTH, number);
            result = sdf.format(c.getTime());

        } catch (ParseException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static int compare2Dates(String date1, String date2) {
        int result = 0;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            java.util.Date utilDate1 = new java.util.Date(sdf.parse(date1).getTime());
            java.util.Date utilDate2 = new java.util.Date(sdf.parse(date2).getTime());
            result = (int) TimeUnit.DAYS.convert(utilDate2.getTime() - utilDate1.getTime(), TimeUnit.MILLISECONDS);

        } catch (ParseException e) {
            e.printStackTrace();
        }
        return result;
    }
}
