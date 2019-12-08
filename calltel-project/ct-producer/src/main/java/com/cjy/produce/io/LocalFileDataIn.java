package com.cjy.produce.io;

import com.cjy.ct.bean.Data;
import com.cjy.ct.bean.DataIn;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class LocalFileDataIn implements DataIn {

    //本地读入文件需要流
    private BufferedReader bufferedReader = null;


    public LocalFileDataIn(String path) {
       setPath(path);
    }

    /**
     * 根据路径读入文件-转换字节流到字符流
     *
     * @param path
     */
    public void setPath(String path) {

        try {
            try {
                bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(path),"utf-8"));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public Object read() {
        return null;
    }

    /**
     * 读入文件数据封装集合
     * @param tClass 每行数据封装类型
     * @param <T>
     * @return
     * @throws IOException
     */
    public <T extends Data> List<T> read(Class<T> tClass) throws IOException {
        List<T> list = new ArrayList<T>();

        String lineData = null;
        //一行行读入数据封装对象
        while ((lineData = bufferedReader.readLine()) != null){
            try {
                //反射创建数据类，封装数据
                T t = tClass.newInstance();
                t.setValue(lineData);
                list.add(t);
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
            return list;
    }

    public void close() throws IOException {
        if (bufferedReader != null) {
            synchronized (Object.class) {
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
            }
        }
    }
}
