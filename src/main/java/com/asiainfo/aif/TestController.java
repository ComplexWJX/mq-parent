package com.asiainfo.aif;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

/**
 * @author WJX
 * @title: TestController
 * @projectName aif
 * @description: TODO
 * @date 2020/7/31 0031
 */
@Controller
public class TestController {
    @RequestMapping("/index")
    public ModelAndView index(){
        ModelAndView modelAndView = new ModelAndView();
        modelAndView.setViewName("thymeleaf");
        modelAndView.addObject("title","注意点");
        return modelAndView;
    }
}
