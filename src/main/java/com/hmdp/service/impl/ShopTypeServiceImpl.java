package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import io.netty.util.internal.StringUtil;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public Result queryTypyList(){
        String cacheCode = "cache:shopType:list:";
        String JSON = stringRedisTemplate.opsForValue().get(cacheCode);

        if (StringUtils.isNotBlank(JSON)) {
            //命中缓存，反序列化后返回
            List<ShopType> typeList = JSONUtil.toList(JSON, ShopType.class);
            return Result.ok(typeList);
        }
        //判断是否命中空值
        if (JSON == null) {
            return Result.ok(Collections.emptyList());
        }

        //缓存未命中，数据库里查询
        List<ShopType> typeList = query().orderByAsc("sort").list();
        if (typeList == null || typeList.isEmpty()) {
            // 避免缓存穿透：可缓存空值，设置短暂 TTL（可选）
            stringRedisTemplate.opsForValue().set(cacheCode, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            return Result.ok(Collections.emptyList());
        }

        //将数据写入缓存
        stringRedisTemplate.opsForValue().set(cacheCode, JSONUtil.toJsonStr(typeList), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return Result.ok(typeList);
    }
}
