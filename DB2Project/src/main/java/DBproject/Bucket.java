package DBproject;

import java.io.Serializable;
import java.util.Vector;

public class Bucket implements Serializable {
    Vector<Integer> bucketIndex;
    Vector<String> bucketIndexPage;
    Vector<Integer> bucketIndexInPage;
    int index;

    public Bucket() {
        bucketIndex = new Vector<Integer>();
        bucketIndexPage = new Vector<String>();
        bucketIndexInPage = new Vector<Integer>();
        this.index = 0;
    }

    public void add(String page, int indexInPage) throws DBAppException {
        int maxIndex;
        if (Integer.parseInt(Page.readConfigFile("MaximumKeysCountinIndexBucket")) == -1)
            throw new DBAppException();
        else
            maxIndex = Integer.parseInt(Page.readConfigFile("MaximumKeysCountinIndexBucket"));
        if (index < maxIndex) {
            bucketIndex.add(index++);
            bucketIndexInPage.add(indexInPage);
            bucketIndexPage.add(page);
        } else {// create overflowBucket}
        }
    }

    public int getBucketSize() {
        return bucketIndex.size();
    }

    public String getPage(int x) {
        return (String) bucketIndexPage.get(x);
    }

    public int getIndexInPage(int x) {
        return (int) bucketIndexInPage.get(x);
    }

}
