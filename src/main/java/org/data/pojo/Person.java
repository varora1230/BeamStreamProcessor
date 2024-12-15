package org.data.pojo;

/**
 * Person represents an individual with basic details such as name, address,
 * date of birth (dob), and age.
 */
public class Person {

    private String uid;

    private String name;

    private String address;

    private String dob;

    private int age;

    //Default constructor.
    public Person() {}

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getDob() {
        return dob;
    }

    public void setDob(String dob) {
        this.dob = dob;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
