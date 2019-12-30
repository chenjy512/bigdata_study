package com.cjy.web.controller;

import com.cjy.web.beans.Calllog;
import com.cjy.web.service.CalllogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;

@Controller
public class CalllogController {

    @Autowired
    private CalllogService calllogService;

    @RequestMapping("query")
    public String query(Map map){
        map.put("hello","hello world!!!");
        return "query";
    }

    @RequestMapping("view")
    public String view(String tel,String calltime,Map map){

        List<Calllog> logs = calllogService.queryByTelAndCalltime(tel,calltime);
        System.out.println(logs.size());
        map.put("calllogs",logs);
        return "view";
    }
}
