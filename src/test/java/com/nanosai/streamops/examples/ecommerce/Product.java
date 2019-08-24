package com.nanosai.streamops.examples.ecommerce;

public class Product {
    public long productId = 0;
    public String productName = null;
    public String productDesc = null;
    public long price  = 0;    // price in smallest currency units - e.g. cents
    public long cost   = 0;    // cost in smallest currency units - e.g. cents

    public Product() {}

    public Product(
            long productId, String productName, String productDesc,
            long price, long cost) {
        this.productId = productId;
        this.productName = productName;
        this.productDesc = productDesc;
        this.price = price;
        this.cost = cost;
    }

}
