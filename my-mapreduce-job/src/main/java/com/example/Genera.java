package com.example;

public class Genera {
    public static void main(String[] args) {
        var job = new MyJob();

        try (var out = new java.io.ObjectOutputStream(new java.io.FileOutputStream("job.ser"))) {
            out.writeObject(job);
            System.out.println("Job serializzato in job.ser");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
