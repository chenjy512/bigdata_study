package com.cjy.udtf;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseUDTF extends GenericUDTF {
    Logger log = LoggerFactory.getLogger(BaseUDTF.class);
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("pname");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("product_json");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    //多条数据数组
    @Override
    public void process(Object[] objects) throws HiveException {
        log.info(objects.toString());

        if(objects == null || objects.length == 0)
            return;
        //取出第一条数据
        String value = objects[0].toString();
        log.info(value);
        if(StringUtils.isBlank(value)){
            return ;
        }else{
            try {
                //转为json数组
                JSONArray ja = new JSONArray(value);
                if (ja == null)
                    return;
                for (int i = 0; i < ja.length(); i++) {
                    String[] result = new String[2];
                    try {
                        // 取出每个的事件名称（ad/facoriters）
                        result[0] = ja.getJSONObject(i).getString("pname");
                        // 取出每一个事件整体
                        result[1] = ja.getString(i);
                    } catch (JSONException e) {
                        continue;
                    }
                    // 将结果返回
                    forward(result);
                }
            } catch (JSONException e) {
                System.out.println(111111);
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
