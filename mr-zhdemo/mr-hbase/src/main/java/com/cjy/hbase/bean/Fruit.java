package com.cjy.hbase.bean;

public class Fruit {

    private String id;
    private String name;
    private String color;

    public Fruit(String id, String name, String color) {
        this.id = id;
        this.name = name;
        this.color = color;
    }

    public Fruit() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }

    @Override
    public String toString() {
        return  id + "\t" + name+"\t"+color;
    }
}
