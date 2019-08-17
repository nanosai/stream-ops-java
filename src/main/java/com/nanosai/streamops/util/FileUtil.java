package com.nanosai.streamops.util;

import java.awt.image.DirectColorModel;
import java.io.File;

public class FileUtil {

    public static boolean deleteDir(String dirPath){
        return deleteDir(new File(dirPath));
    }

    public static boolean deleteDir(File dir){
        File[] files = dir.listFiles();
        if(files != null){
            for(File file : files){
                if(file.isDirectory()){
                    deleteDir(dir);
                } else {
                    file.delete();
                }
            }
        }
        return dir.delete();
    }

    public static boolean resetDir(File dir){
        if(!deleteDir(dir)){
            return false;
        }
        return dir.mkdirs();

    }
}
