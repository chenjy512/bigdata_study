package com.cjy.study1.beans;

public class RowData {
//    private String tableName;
    private String rowKey;
    private String cf;
    private String[] cns;
    private String[] values;

    public RowData(String rowKey, String cf, String[] cns, String[] values) {
        this.rowKey = rowKey;
        this.cf = cf;
        this.cns = cns;
        this.values = values;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getCf() {
        return cf;
    }

    public void setCf(String cf) {
        this.cf = cf;
    }

    public String[] getCns() {
        return cns;
    }

    public void setCns(String[] cns) {
        this.cns = cns;
    }

    public String[] getValues() {
        return values;
    }

    public void setValues(String[] values) {
        this.values = values;
    }
}
