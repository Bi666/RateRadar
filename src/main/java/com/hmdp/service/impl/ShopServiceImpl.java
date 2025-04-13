package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSON;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static cn.hutool.core.thread.GlobalThreadPool.submit;
import static com.hmdp.utils.RedisConstants.*;

@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public Result queryById(Long id){
        //缓存穿透
        //Shop shop = queryWithPassThrough(id);

        //互斥锁解决缓存击穿
        Shop shop = queryWithMutex(id);
        if(shop == null){
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    /**
     * 解决缓存穿透
     * @param id
     * @return
     */
    public Shop queryWithLogicalExpire(Long id){
        String shopKey = CACHE_SHOP_KEY + id;
        //1.redis查缓存
        String shopJson = stringRedisTemplate.opsForValue().get(shopKey);
        // 2.判断缓存是否命中
        if (StrUtil.isBlank(shopJson)) {
            //2.1.未命中，返回空
            return null;
        }
        //2.2.命中，先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        //3.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            //3.1未过期，直接返回
            return shop;
        }
        //4.过期，缓存重建
        //4.1.获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        //4.2.判断是否获取成功
        if (tryLock(lockKey)) {
            //4.4.成功，开启独立线程，实现缓存重建
            CACHE_EXECUTOR.submit(()->{
                try {
                    this.saveShop2Redis(id, 20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unlock(lockKey);
                }
            });
        }
        //5.返回过期商铺信息
        return shop;
    }

    private static final ExecutorService CACHE_EXECUTOR = Executors.newFixedThreadPool(10);

    /**
     * 解决缓存击穿
     * @param id 商铺ID
     * @return Shop对象
     */
    public Shop queryWithMutex(Long id) {
        String shopKey = CACHE_SHOP_KEY + id;
        String lockKey = LOCK_SHOP_KEY + id;

        try {
            // 1. 先查缓存
            String shopJson = stringRedisTemplate.opsForValue().get(shopKey);
            if (!StrUtil.isBlank(shopJson)) {
                return JSONUtil.toBean(shopJson, Shop.class);
            }
            if (shopJson != null) {
                return null; // 命中的是空值
            }

            // 2. 自旋尝试获取锁
            int retry = 0, maxRetry = 50;
            while (retry++ < maxRetry) {
                if (!tryLock(lockKey)) {
                    Thread.sleep(50);
                    continue;
                }

                try {
                    // 成功获取锁后，double check，防止重复重建缓存
                    shopJson = stringRedisTemplate.opsForValue().get(shopKey);
                    if (!StrUtil.isBlank(shopJson)) {
                        return JSONUtil.toBean(shopJson, Shop.class);
                    }
                    if (shopJson != null) {
                        return null;
                    }

                    // 查数据库
                    Shop shop = getById(id);
                    if (shop == null) {
                        stringRedisTemplate.opsForValue()
                                .set(shopKey, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                        return null;
                    }
                    // 写入缓存
                    stringRedisTemplate.opsForValue()
                            .set(shopKey, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
                    return shop;
                } finally {
                    unlock(lockKey);
                }
            }

            // 超过最大重试次数
            throw new RuntimeException("获取锁失败，超过最大重试次数");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 解决缓存穿透
     * @param id
     * @return
     */
    public Shop queryWithPassThrough(Long id){
        // 从 redis 查缓存
        String shopKey = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(shopKey);

        // 判断缓存是否命中
        if (!StrUtil.isBlank(shopJson)) {
            // 命中，返回缓存数据
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //判断命中的是否是空值
        if (shopJson != null) {
            return null;
        }

        // 未命中，查询数据库
        Shop shop = getById(id);
        if (shop == null) {
            stringRedisTemplate.opsForValue().set(shopKey,"", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }

        // 写入缓存
        stringRedisTemplate.opsForValue().set(shopKey, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }

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

    public void saveShop2Redis(Long id, Long expireSeconds) {
        // 1. 查询店铺数据
        Shop shop = getById(id);
        // 2. 封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //存入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    @Transactional
    public Result updateShop(Shop shop){
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        //1.更新数据库
        updateById(shop);
        //2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }
}
