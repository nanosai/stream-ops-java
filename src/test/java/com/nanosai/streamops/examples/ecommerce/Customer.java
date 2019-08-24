package com.nanosai.streamops.examples.ecommerce;

public class Customer {
    public long customerId = 0;
    public String name = null;

    public Customer() {}

    public Customer(long customerId, String name) {
        this.customerId = customerId;
        this.name = name;
    }
}
