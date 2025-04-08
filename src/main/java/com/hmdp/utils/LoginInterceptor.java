package com.hmdp.utils;

import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

public class LoginInterceptor implements HandlerInterceptor {
    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        //移除用户
        UserHolder.removeUser();
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        //获取session中的用户
        HttpSession session = request.getSession();
        //判断用户是否存在
        Object user = session.getAttribute("user");
        if (user == null) {
            //不存在，拦截--返回401状态码
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            return false;
        }

        //存在，保存用户信息到ThreadLocal
        UserHolder.saveUser((User) user);
        return true;
    }
}
