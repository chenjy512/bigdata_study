package com.cjy.udf;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.spi.LoggerFactory;
import org.slf4j.Logger;

public class Lower extends UDF {

    Logger log = org.slf4j.LoggerFactory.getLogger(Lower.class);
    public String evaluate (final String s) {
        log.info(s);
        if (s == null) {
            return null;
        }

        return s.toLowerCase();
    }

    public static void main(String[] args) {
        System.out.println(new Lower().evaluate("ASFSSFFA"));
    }
}