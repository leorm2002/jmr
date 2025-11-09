package com.example;

import it.jmr.common.models.JobConfiguration;

public class TestJob {

    public static void main(String[] args) {
        JobConfiguration<?, ?, ?> jobConfig = new MyJob();
        System.out.println("Job Configuration created: " + jobConfig.getClass().getName());
    
    

        
    
    
    }

}
