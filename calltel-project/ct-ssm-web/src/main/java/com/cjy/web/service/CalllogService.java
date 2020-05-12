package com.cjy.web.service;

import com.cjy.web.beans.Calllog;

import java.util.List;

public interface CalllogService {
    List<Calllog> queryByTelAndCalltime(String tel, String calltime);
}
