package com.nanosai.streamops.examples.ecommerce;

import com.nanosai.rionops.rion.read.RionReader;
import com.nanosai.streamops.storage.file.IRecordProcessor;

import java.util.ArrayList;
import java.util.List;

public class ECommerceReport implements IRecordProcessor {

    private List<Product>  products  = null;
    private List<Customer> customers = null;

    private List<ProductRevenueSum> productRevenueSums = new ArrayList<>();



    private long totalRevenue = 0;
    public long recordCount = 0;


    public ECommerceReport(List<Product> products, List<Customer> customers) {
        this.products = products;
        this.customers = customers;

        for(int i=0; i< this.products.size(); i++) {
            this.productRevenueSums.add(new ProductRevenueSum());
        }
    }

    @Override
    public boolean process(long recordOffset, RionReader rionReader) {

        //System.out.println("[" + recordOffset + "][" + rionReader.fieldType +"]["+rionReader.fieldLength +"]");

        this.recordCount++;

        rionReader.moveInto();
        while(rionReader.hasNext()){
            //rionReader.nextParse();
            //long orderItemId = rionReader.readInt64();

            rionReader.nextParse();
            long productId = rionReader.readInt64();

            rionReader.nextParse();
            long orderId = rionReader.readInt64();

            rionReader.nextParse();
            long customerId = rionReader.readInt64();

            //System.out.println("   {[" + orderItemId + "][" + productId + "][" + orderId + "][" + customerId + "]}");
            //System.out.println("   {[" + productId + "][" + orderId + "][" + customerId + "]}");

            Product product = this.products.get((int) productId);
            ProductRevenueSum productRevenueSum = this.productRevenueSums.get((int) productId);
            productRevenueSum.revenueSum += product.price;

            this.totalRevenue += product.price;
        }
        rionReader.moveOutOf();


        return true;
    }



    public void printResult() {
        System.out.println("E-Commerce Report:");

        System.out.println("Total revenue: " + this.totalRevenue);

        for(int i=0; i<this.productRevenueSums.size();i++) {
            Product           product           = this.products.get(i);
            ProductRevenueSum productRevenueSum = this.productRevenueSums.get(i);

            System.out.println(product.productName + " : " + productRevenueSum.revenueSum);
        }
    }
}
