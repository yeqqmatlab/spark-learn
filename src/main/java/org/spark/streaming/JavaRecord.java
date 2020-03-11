package org.spark.streaming;

/**
 * created by yqq 2020/3/9
 */
public class JavaRecord implements java.io.Serializable{
    private String word;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }
}
