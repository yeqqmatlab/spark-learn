package bean;

import java.io.Serializable;

public class MethodJson implements Serializable {

    private int methodId;

    private int associationNum;

    public int getMethodId() {
        return methodId;
    }

    public void setMethodId(int methodId) {
        this.methodId = methodId;
    }

    public int getAssociationNum() {
        return associationNum;
    }

    public void setAssociationNum(int associationNum) {
        this.associationNum = associationNum;
    }


    @Override
    public String toString() {
        return "Methodson{" +
                "methodId=" + methodId +
                ", associationNum=" + associationNum +
                '}';
    }
}
