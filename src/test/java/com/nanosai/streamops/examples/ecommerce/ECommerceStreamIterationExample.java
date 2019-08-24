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
        String streamId  = "e-commerce-example-1";
        String streamDir = "data/" + streamId;
        FileUtil.resetDir(new File(streamDir));

        StreamStorageFS streamStorage = new StreamStorageFS(streamId, streamDir, 1024 * 1024);
        streamStorage.openForAppend();

        List<Product> products = new ArrayList<>();
        products.add(new Product(0, "Soda", "Cold softdrink", 100, 50));
        products.add(new Product(1, "Choc", "Chocolate bar", 90, 47));
        products.add(new Product(2, "Chip", "Potatoe chips", 150, 83));

        List<Customer> customers = new ArrayList<>();
        customers.add(new Customer(0, "John Doe"));
        customers.add(new Customer(1, "Jane Dee"));

        long noOfOrders = 10;
        int noOfOrderItems = 1_000_000;

        byte[] rionRecord = new byte[1024];
        RionWriter rionWriter = new RionWriter()
                .setNestedFieldStack(new int[16])
                .setDestination(rionRecord,0);

        for(int i=0; i<noOfOrderItems; i++) {
            long productId  = (long) (Math.random() * (double) products.size());
            long customerId = (long) (Math.random() * (double) customers.size());
            long orderId    = (long) (Math.random() * (double) noOfOrders);

            rionWriter.setDestination(rionRecord, 0);
            //create an order item record
            rionWriter.writeObjectBeginPush(2);
            rionWriter.writeInt64( i);              //orderItemId
            rionWriter.writeInt64(productId);       //productId
            rionWriter.writeInt64(orderId);         //orderId
            rionWriter.writeInt64(customerId);      //customerId

            // date can be obtained from Order object. I can add Order objects later on.
            //GregorianCalendar dateTime = new GregorianCalendar();
            //rionWriter.writeUtc( dateTime, 9);  //date-time in UTC, 9 bytes = including milliseconds
            rionWriter.writeObjectEndPop();

            streamStorage.appendRecord(rionRecord, 0, rionWriter.index);
        }

        streamStorage.closeForAppend();



        byte[] recordBuffer = new byte[(int) streamStorage.getStorageFileBlockMaxSize()];

        ECommerceReport report = new ECommerceReport(products, customers);
        streamStorage.iterate(recordBuffer, report);

        report.printResult();

    }


}