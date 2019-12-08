package com.cjy.produce.io;

import com.cjy.ct.bean.DataOut;

import java.io.*;

public class LocalFileDataOut implements DataOut {

    private PrintWriter pw = null;

    public LocalFileDataOut(String path) {
        setPath(path);
    }

    public void setPath(String path) {
        try {
            try {
                pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path),"utf-8"));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void write(Object data) {
        write(data.toString());
    }

    public void write(String data) {
        pw.write(data+"\n");

        pw.flush();
    }

    public void close() throws IOException {
        if(pw != null){
            pw.close();
        }
    }
}
