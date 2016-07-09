package com.mr.utils;

/**
 * Created by TakiyaHideto on 16/2/15.
 */
public class FeaEngUtil {
    public static String transformReservePrice(String reservePrice){
        String reservePriceCate = "";
        int reservePriceInt = Integer.parseInt(reservePrice);
        if (reservePriceInt>=0 && reservePriceInt<20)
            reservePriceCate = "0";
        else if (reservePriceInt>=20 && reservePriceInt<90)
            reservePriceCate = "1";
        else if (reservePriceInt>=90 && reservePriceInt<120)
            reservePriceCate = "2";
        else if (reservePriceInt>=120 && reservePriceInt<210)
            reservePriceCate = "3";
        else if (reservePriceInt>=210 && reservePriceInt<300)
            reservePriceCate = "4";
        else if (reservePriceInt>=300 && reservePriceInt<350)
            reservePriceCate = "5";
        else if (reservePriceInt>=350 && reservePriceInt<740)
            reservePriceCate = "6";
        else if (reservePriceInt>=740 && reservePriceInt<820)
            reservePriceCate = "7";
        else if (reservePriceInt>=820 && reservePriceInt<825)
            reservePriceCate = "8";
        else
            reservePriceCate = "9";
        return  reservePriceCate;
    }

//        public String transformYoyiCost(String yoyiCost){
//            String yoyiCost
//        }
}

