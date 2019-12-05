package com.cjy.constans;

public class Constans {

    public static String NAMA_SPACE = "weibo";

    //微博内容信息表
    public static String CONTENT_TABLE="weibo:content";
    public static String CONTENT_TABLE_CF="info";
    public static String CONTENT_TABLE_CN="content";
    public static Integer CONTENT_TABLE_VERSIONS = 1;

    //用于关系表
    public static String RELATION_TABLE="weibo:relation";
    public static String RELATION_TABLE_CF1="attends";
    public static String RELATION_TABLE_CF2="fans";
    public static Integer RELATION_TABLE_VERSIONS=1;

    //收件箱
    public static String INBOX_TABLE="weibo:inbox";
    public static String INBOX_TABLE_CF="info";
    public static Integer INBOX_TABLE_VERSIONS=3;

}
