package com.cjy.web.service.impl;

import com.cjy.web.beans.Calllog;
import com.cjy.web.dao.CalllogDao;
import com.cjy.web.service.CalllogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class CalllogServiceImpl implements CalllogService {

    @Autowired
    private CalllogDao calllogDao;
    @Override
    public List<Calllog> queryByTelAndCalltime(String tel, String calltime) {

        Map<String,String> map = new HashMap<>(2);

        if(!StringUtils.isEmpty(tel)){
            map.put("tel",tel);
        }
        if(!StringUtils.isEmpty(calltime)){
            calltime = calltime.substring(0,4);
            map.put("year",calltime);
        }
        return calllogDao.queryByTelAndCalltime(map);
    }
}
