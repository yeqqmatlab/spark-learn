package bean;

import java.io.Serializable;

public class AssociationMethod implements Serializable {


    private int associationId;

    private int methodId;

    private int associationNum;

    private String methodName;


    public int getAssociationId() {
        return associationId;
    }

    public void setAssociationId(int associationId) {
        this.associationId = associationId;
    }

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

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    @Override
    public String toString() {
        return "AssociationMethod{" +
                "associationId=" + associationId +
                ", methodId=" + methodId +
                ", associationNum=" + associationNum +
                ", methodName='" + methodName + '\'' +
                '}';
    }
}
