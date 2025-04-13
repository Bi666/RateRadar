package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long expire, TimeUnit timeUnit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), expire, timeUnit);
    }

    //Logical expire
    public void setWithExpire(String key, Object value, Long expire, TimeUnit timeUnit) {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(timeUnit.toSeconds(expire)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 解决缓存穿透
     * @param id
     * @return
     */
    public <R, ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long expire, TimeUnit timeUnit) {
        // 从 redis 查缓存
        String shopKey = keyPrefix + id;
        String Json = stringRedisTemplate.opsForValue().get(shopKey);

        // 判断缓存是否命中
        if (!StrUtil.isBlank(Json)) {
            // 命中，返回缓存数据
            return JSONUtil.toBean(Json, type);
        }
        //判断命中的是否是空值
        if (Json != null) {
            return null;
        }

        // 未命中，查询数据库
        R r = dbFallback.apply(id);
        if (r == null) {
            stringRedisTemplate.opsForValue().set(shopKey,"", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }

        // 写入缓存
        this.set(shopKey, r, expire, timeUnit);
        return r;
    }

    /**
     * 解决缓存击穿
     * @param id
     * @return
     */
    public <R, ID> R queryWithLogicalExpire(
            String keyPrefix,ID id, Class<R> type, Function<ID, R> dbFallback, Long expire, TimeUnit timeUnit) {
        String shopKey = keyPrefix + id;
        //1.redis查缓存
        String Json = stringRedisTemplate.opsForValue().get(shopKey);
        // 2.判断缓存是否命中
        if (StrUtil.isBlank(Json)) {
            //2.1.未命中，返回空
            return null;
        }
        //2.2.命中，先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(Json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //3.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            //3.1未过期，直接返回
            return r;
        }
        //4.过期，缓存重建
        //4.1.获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        //4.2.判断是否获取成功
        if (tryLock(lockKey)) {
            //4.4.成功，开启独立线程，实现缓存重建
            CACHE_EXECUTOR.submit(()->{
                try {
                    //查询数据库
                    R r1 = dbFallback.apply(id);
                    //写入redis
                    this.setWithExpire(lockKey, r1, expire, timeUnit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unlock(lockKey);
                }
            });
        }
        //5.返回过期信息
        return r;
    }

    private static final ExecutorService CACHE_EXECUTOR = Executors.newFixedThreadPool(10);

    /**
     * 尝试获取互斥锁
     * @param key
     * @return
     */
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 释放锁
     * @param key
     * @return
     */
    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }
}
