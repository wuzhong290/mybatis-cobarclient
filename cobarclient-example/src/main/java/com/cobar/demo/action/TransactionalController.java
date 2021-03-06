package com.cobar.demo.action;

import com.alibaba.fastjson.JSONObject;
import com.cobar.demo.model.UserModel;
import com.cobar.demo.service.TradeService;
import com.cobar.demo.service.UserService;
import javax.annotation.Resource;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class TransactionalController {
    @Resource
    UserService userService;
    @Resource
    TradeService tradeService;

    @RequestMapping(value = {"/"}, method = RequestMethod.GET)
    public String index(ModelMap model) {
        model.addAttribute("message", "Hello world!<br/>");
        return "hello";
    }

    @ResponseBody
    @RequestMapping(value = {"/t0"}, method = RequestMethod.GET)
    public String add0() {
        UserModel userMode=new UserModel();
        userMode.setUserId("1");
        userMode.setUserNick("guanghua2cc");
        try{
            userService.insertUserNotTransaction(userMode);
        }catch (Exception e){
            e.printStackTrace();
        }
        return JSONObject.toJSONString(userService.getUserList());
    }

    //手动事务测试,向用户表插入一个用户,向订单表插入一笔订单
    //订单ID跟用户ID都是主键,如果有一个出现重复则全部回滚
    @ResponseBody
    @RequestMapping(value = {"/t1"}, method = RequestMethod.GET)
    public String add() {
        UserModel userMode=new UserModel();
        userMode.setUserId("1");
        userMode.setUserNick("guanghua2cc");
        userService.insertUser(userMode);
        return JSONObject.toJSONString(userService.getUserList());
    }
    //注解事务测试
    @ResponseBody
    @RequestMapping(value = {"/t2"}, method = RequestMethod.GET)
    public String add2() {
        UserModel userMode=new UserModel();
        userMode.setUserId("1");
        userMode.setUserNick("guanghua2cc");
        try{
            userService.insertUserAnnotations(userMode);
        }catch (Exception e){
            e.printStackTrace();
        }
        return JSONObject.toJSONString(userService.getUserList());
    }

    @ResponseBody
    @RequestMapping(value = {"/t3"}, method = RequestMethod.GET)
    public String add3() {
        userService.clean("guanghua2cc");
        return JSONObject.toJSONString(userService.getUserList());
    }

    @ResponseBody
    @RequestMapping(value = {"/t4"}, method = RequestMethod.GET)
    public String add4() {
        UserModel userMode=new UserModel();
        userMode.setUserId("1");
        userMode.setUserNick("guanghua2cc");
        try{
            userService.insertUserTemplate(userMode);
        }catch (Exception e){
            e.printStackTrace();
        }
        return JSONObject.toJSONString(userService.getUserList());
    }
}