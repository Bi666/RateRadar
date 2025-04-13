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
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.apache.ibatis.annotations.Case;
import org.springframework.beans.factory.annotation.Autowired;
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

    @Autowired
    private CacheClient cacheClient;

    public Result queryById(Long id){
        //缓存穿透
//        Shop shop = cacheClient.queryWithPassThrough(
//                CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //互斥锁解决缓存击穿
//        Shop shop = queryWithMutex(id);

        //Logical expire解决缓存击穿
        //需要缓存预热！
        Shop shop = cacheClient.queryWithLogicalExpire(
                CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        if(shop == null){
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

//    /**
//     * 互斥锁解决缓存击穿
//     * @param id 商铺ID
//     * @return Shop对象
//     */
//    public Shop queryWithMutex(Long id) {
//        String shopKey = CACHE_SHOP_KEY + id;
//        String lockKey = LOCK_SHOP_KEY + id;
//
//        try {
//            // 1. 先查缓存
//            String shopJson = stringRedisTemplate.opsForValue().get(shopKey);
//            if (!StrUtil.isBlank(shopJson)) {
//                return JSONUtil.toBean(shopJson, Shop.class);
//            }
//            if (shopJson != null) {
//                return null; // 命中的是空值
//            }
//
//            // 2. 自旋尝试获取锁
//            int retry = 0, maxRetry = 50;
//            while (retry++ < maxRetry) {
//                if (!tryLock(lockKey)) {
//                    Thread.sleep(50);
//                    continue;
//                }
//
//                try {
//                    // 成功获取锁后，double check，防止重复重建缓存
//                    shopJson = stringRedisTemplate.opsForValue().get(shopKey);
//                    if (!StrUtil.isBlank(shopJson)) {
//                        return JSONUtil.toBean(shopJson, Shop.class);
//                    }
//                    if (shopJson != null) {
//                        return null;
//                    }
//
//                    // 查数据库
//                    Shop shop = getById(id);
//                    if (shop == null) {
//                        stringRedisTemplate.opsForValue()
//                                .set(shopKey, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
//                        return null;
//                    }
//                    // 写入缓存
//                    stringRedisTemplate.opsForValue()
//                            .set(shopKey, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
//                    return shop;
//                } finally {
//                    unlock(lockKey);
//                }
//            }
//
//            // 超过最大重试次数
//            throw new RuntimeException("获取锁失败，超过最大重试次数");
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//    }

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
