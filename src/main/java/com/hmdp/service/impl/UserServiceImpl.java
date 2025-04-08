package com.hmdp.service.impl;

import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpSession;

import java.time.LocalDateTime;

import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Autowired
    private UserMapper userMapper;

    public Result sendcode(String phone, HttpSession session) {
        //校验手机号，符合手机号格式
        if (RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("手机号格式错误");
        }
        //生成验证码
        String code = RandomUtil.randomNumbers(6);

        //保存验证码到session
        session.setAttribute("code", code);
        log.info("发送短信验证码成功{}", code);

        //发送验证码
        return Result.ok();
    }


    public Result login(LoginFormDTO loginForm, HttpSession session){
         //校验手机号
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("手机号格式错误");
        }

        //校验验证码
        String cashCode = session.getAttribute("code").toString();
        if (cashCode != null && !cashCode.equals(loginForm.getCode())){
            return Result.fail("验证码错误");
        }

        // 使用 MyBatis-Plus 根据手机号查询用户
        User user = query().eq("phone", phone).one();

        //判断用户是否存在
        if (user == null){
            //不存在-创建用户
            user = new User();
            user.setPhone(phone);
            user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
            user.setCreateTime(LocalDateTime.now());
            user.setUpdateTime(LocalDateTime.now());

            // 使用 MyBatis-Plus 插入用户
            save(user);
        }

        //保存用户信息到session
        session.setAttribute("user", user);
        return Result.ok();
    }
}
