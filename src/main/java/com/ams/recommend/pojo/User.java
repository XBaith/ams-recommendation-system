package com.ams.recommend.pojo;

public class User {
    private String userId;
    private int sex;
    private int age;
    private String job;
    private String education;

    public User(){}

    public User(String userId, int sex, int age, String job, String education) {
        this.userId = userId;
        this.sex = sex;
        this.age = age;
        this.job = job;
        this.education = education;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public int getSex() {
        return sex;
    }

    public void setSex(int sex) {
        this.sex = sex;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public String getEducation() {
        return education;
    }

    public void setEducation(String education) {
        this.education = education;
    }
}
