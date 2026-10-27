package com.example.spark.model;

import java.io.Serializable;

/**
 * Java Bean used with Spark's Dataset<Employee> (typed API).
 *
 * Requirements for Spark Encoders.bean():
 *   - public no-arg constructor (Spark instantiates via reflection)
 *   - public getters/setters (Spark maps CSV column names → field names)
 *   - Serializable (Spark may serialize objects when shuffling between partitions)
 *
 * Field names must match CSV column headers exactly (case-sensitive).
 */
public class Employee implements Serializable {

    private int id;
    private String name;
    private String department;
    private double salary;
    private int age;

    public Employee() {}

    public Employee(int id, String name, String department, double salary, int age) {
        this.id = id;
        this.name = name;
        this.department = department;
        this.salary = salary;
        this.age = age;
    }

    public int getId() { return id; }
    public void setId(int id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getDepartment() { return department; }
    public void setDepartment(String department) { this.department = department; }

    public double getSalary() { return salary; }
    public void setSalary(double salary) { this.salary = salary; }

    public int getAge() { return age; }
    public void setAge(int age) { this.age = age; }

    @Override
    public String toString() {
        return "Employee{id=" + id + ", name='" + name + "', dept='" + department +
               "', salary=" + salary + ", age=" + age + "}";
    }
}
