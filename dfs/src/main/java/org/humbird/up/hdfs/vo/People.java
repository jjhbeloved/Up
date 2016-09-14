package org.humbird.up.hdfs.vo;

import javax.persistence.*;
import java.io.Serializable;

/**
 * Created by david on 16/9/11.
 */
@Entity
@Table(name = "t_people_1000")
public class People implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private long id;

    @Column(length = 20, nullable = false, name = "name", unique = true)
    private String name;

    private int age;

    @Column(length = 1, nullable = false, name = "sex")
    private byte sex;
    private String birthday;
    private String comments;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public byte getSex() {
        return sex;
    }

    public void setSex(byte sex) {
        this.sex = sex;
    }

    public String getBirthday() {
        return birthday;
    }

    public void setBirthday(String birthday) {
        this.birthday = birthday;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

}
