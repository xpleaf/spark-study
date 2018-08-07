package cn.xpleaf.bigdata.spark.java.core.domain;

import java.io.Serializable;

/**
 * stu_info(sid, name, birthday, class)
 */
public class Student implements Serializable {
    private String sid;
    private String name;
    private String birthday;
    private String className;

    public Student(String sid, String name, String birthday, String className) {
        this.sid = sid;
        this.name = name;
        this.birthday = birthday;
        this.className = className;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getBirthday() {
        return birthday;
    }

    public void setBirthday(String birthday) {
        this.birthday = birthday;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    @Override
    public String toString() {
        return sid + ' ' + name + ' ' + birthday + ' ' + className;
    }
}
