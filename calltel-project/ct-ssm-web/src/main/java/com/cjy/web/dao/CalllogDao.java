package com.cjy.web.dao;

import com.cjy.web.beans.Calllog;

import java.util.List;
import java.util.Map;

public interface CalllogDao {

    List<Calllog> queryByTelAndCalltime(Map<String, String> map);
}
