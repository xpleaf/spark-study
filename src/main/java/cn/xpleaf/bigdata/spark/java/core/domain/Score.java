package cn.xpleaf.bigdata.spark.java.core.domain;

import java.io.Serializable;

/**
 * stu_score(sid, chinese, english, math)
 */
public class Score implements Serializable {
    private String sid;
    private float chinese;
    private float english;
    private float math;

    public Score(String sid, float chinese, float english, float math) {
        this.sid = sid;
        this.chinese = chinese;
        this.english = english;
        this.math = math;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public float getChinese() {
        return chinese;
    }

    public void setChinese(float chinese) {
        this.chinese = chinese;
    }

    public float getEnglish() {
        return english;
    }

    public void setEnglish(float english) {
        this.english = english;
    }

    public float getMath() {
        return math;
    }

    public void setMath(float math) {
        this.math = math;
    }

    @Override
    public String toString() {
        return sid + ' ' + chinese + ' ' + english + ' ' + math;
    }
}
