package com.nanosai.streamops.util;

public class HexUtil {

    public static String toHex(long offset) {
        StringBuilder builder = new StringBuilder();

        for(int i=60; i >= 0; i-=4){
            char digitChar='0';
            long digit = offset >> i;
            digit &= 0xF;
            switch((int) digit) {
                case 0 : digitChar = '0'; break;
                case 1 : digitChar = '1'; break;
                case 2 : digitChar = '2'; break;
                case 3 : digitChar = '3'; break;
                case 4 : digitChar = '4'; break;
                case 5 : digitChar = '5'; break;
                case 6 : digitChar = '6'; break;
                case 7 : digitChar = '7'; break;
                case 8 : digitChar = '8'; break;
                case 9 : digitChar = '9'; break;
                case 10 : digitChar = 'A'; break;
                case 11 : digitChar = 'B'; break;
                case 12 : digitChar = 'C'; break;
                case 13 : digitChar = 'D'; break;
                case 14 : digitChar = 'E'; break;
                case 15 : digitChar = 'F'; break;
            }
            builder.append(digitChar);
        }

        return builder.toString();
    }


    public static long fromHex(String hexNumber){
        long returnValue = 0;

        for(int i=0; i < hexNumber.length(); i++){
            char digit = hexNumber.charAt(i);

            long digitValue = 0;
            switch(digit){
                case '0': digitValue =  0; break;
                case '1': digitValue =  1; break;
                case '2': digitValue =  2; break;
                case '3': digitValue =  3; break;
                case '4': digitValue =  4; break;
                case '5': digitValue =  5; break;
                case '6': digitValue =  6; break;
                case '7': digitValue =  7; break;
                case '8': digitValue =  8; break;
                case '9': digitValue =  9; break;
                case 'A': digitValue = 10; break;
                case 'B': digitValue = 11; break;
                case 'C': digitValue = 12; break;
                case 'D': digitValue = 13; break;
                case 'E': digitValue = 14; break;
                case 'F': digitValue = 15; break;
            }

            returnValue <<=4;
            returnValue |=digitValue;
        }

        return returnValue;
    }

}
