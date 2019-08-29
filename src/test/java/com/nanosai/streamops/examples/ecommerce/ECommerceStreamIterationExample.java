package com.nanosai.streamops.examples.ecommerce;

import com.nanosai.rionops.rion.RionFieldTypes;
import com.nanosai.rionops.rion.write.RionWriter;
import com.nanosai.streamops.storage.file.StreamStorageFS;
import com.nanosai.streamops.util.FileUtil;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;

public class ECommerceStreamIterationExample {


    public static void main(String[] args) throws IOException {
        List<Product> products = new ArrayList<>();
        products.add(new Product(0, "Soda ", "Cold softdrink", 100, 50));
        products.add(new Product(1, "Choc ", "Chocolate bar", 90, 47));
        products.add(new Product(2, "Chips", "Potatoe chips", 150, 83));

        List<Customer> customers = new ArrayList<>();
        customers.add(new Customer(0, "John Doe"));
        customers.add(new Customer(1, "Jane Dee"));


        String streamId  = "e-commerce-example-2";
        String streamDir = "data/" + streamId;

        StreamStorageFS streamStorage = new StreamStorageFS(streamId, streamDir, 1024 * 1024);

        byte[] recordBuffer = new byte[(int) streamStorage.getStorageFileBlockMaxSize()];

        ECommerceReport report = new ECommerceReport(products, customers);

        long startTime = System.currentTimeMillis();
        streamStorage.iterate(recordBuffer, report);
        long endTime = System.currentTimeMillis();
        long fullTime = endTime - startTime;

        System.out.println("Time   : " + fullTime);
        System.out.println("Records: " + report.recordCount);

        System.out.println("Speed  :" + report.recordCount * 1000 / fullTime);

        report.printResult();

    }


}
