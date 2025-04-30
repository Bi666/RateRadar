package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Autowired
    private ISeckillVoucherService seckillVoucherService;
    @Autowired
    private RedisIdWorker redisIdWorker;
    @Autowired
    StringRedisTemplate stringRedisTemplate;
    @Autowired
    RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }
    // 创建线程池
    private static final ExecutorService SECKILL_ORDER_ECECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init() {
        // 创建消费者组，如果已经存在则忽略错误
        try {
            stringRedisTemplate.opsForStream().createGroup("stream.orders", ReadOffset.latest(), "g1");
            log.info("成功创建消费者组 g1");
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("BUSYGROUP")) {
                log.warn("消费者组 g1 已存在，无需重复创建");
            } else {
                log.error("创建消费者组失败", e);
            }
        }
        // 提交线程任务
        SECKILL_ORDER_ECECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {
        String queueName = "stream.orders";
        @Override
        public void run() {
            while (true) {
                try {
                    //1. 获取消息队列中的订单信息
                    //XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
                    List<MapRecord<String, Object, Object>> readList = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //2.判断消息获取是否成功
                    if (readList == null || readList.isEmpty()) {
                        //获取失败，继续下一次循环
                        continue;
                    }
                    //3.解析订单信息
                    MapRecord<String, Object, Object> record = readList.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                    //3.获取成功，下单
                    handleVoucherOrder(voucherOrder);
                    //4.ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());

                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    //1. 获取pending-list中的订单信息
                    //XREADGROUP GROUP g1 c1 COUNT 1 STREAMS stream.orders 0
                    List<MapRecord<String, Object, Object>> readList = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //2.判断消息获取是否成功
                    if (readList == null || readList.isEmpty()) {
                        //获取失败，pending-list没有异常消息，结束循环
                        break;
                    }
                    //3.解析订单信息
                    MapRecord<String, Object, Object> record = readList.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                    //3.获取成功，下单
                    handleVoucherOrder(voucherOrder);
                    //4.ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());

                } catch (Exception e) {
                    log.error("处理pending-list订单异常", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    }

//    // 创建阻塞队列
//    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
//    private class VoucherOrderHandler implements Runnable{
//
//        @Override
//        public void run() {
//            while (true){
//                //1. 获取阻塞队列中的订单信息
//                try {
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    //2. 创建订单
//                    handleVoucherOrder(voucherOrder);
//                } catch (Exception e) {
//                    log.error("处理订单异常", e);
//                }
//            }
//        }
//    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        //创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = lock.tryLock();
        if (!isLock){
            log.error("不允许重复下单");
            return;
        }
        try {
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }

    private IVoucherOrderService proxy;

    public Result seckillVoucher(Long voucherId){
        Long userId = UserHolder.getUser().getId();
        //获取当前时间戳
        long currentTime = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC) * 1000; // 转换为毫秒时间戳
        //获取订单id
        long orderId = redisIdWorker.nextId("order");
        // 1. 执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId), String.valueOf(currentTime)
        );

        //2. 判断是否执行成功
        int r = result.intValue();
        if (r != 0) {
            //2.1 不为0，没有购买资格
            if (r == 1) {
                return Result.fail("库存不足");
            } else if (r == 2) {
                return Result.fail("不能重复下单");
            } else if (r == 3) {
                return Result.fail("秒杀尚未开始");
            } else if (r == 4) {
                return Result.fail("秒杀已经结束");
            }
            return Result.fail("下单失败");
        }
        //3. 获取代理对象（事务）
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //4. 返回订单id
        return Result.ok(orderId);
    }

//    /**
//     * 阻塞队列 bloxking queue
//     * @param voucherId
//     * @return
//     */
//    public Result seckillVoucher(Long voucherId){
//        Long userId = UserHolder.getUser().getId();
//        //获取当前时间戳
//        long currentTime = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC) * 1000; // 转换为毫秒时间戳
//        // 1. 执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString(), String.valueOf(currentTime)
//        );
//
//        //2. 判断是否执行成功
//        int r = result.intValue();
//        if (r != 0) {
//            //2.1 不为0，没有购买资格
//            if (r == 1) {
//                return Result.fail("库存不足");
//            } else if (r == 2) {
//                return Result.fail("不能重复下单");
//            } else if (r == 3) {
//                return Result.fail("秒杀尚未开始");
//            } else if (r == 4) {
//                return Result.fail("秒杀已经结束");
//            }
//            return Result.fail("下单失败");
//        }
//
//        //2.2 成功， 把下单信息保存代阻塞队列
//        long orderId = redisIdWorker.nextId("order");
//        // 创建订单
//        VoucherOrder voucherOrder = VoucherOrder.builder()
//                .id(orderId)
//                .userId(userId)
//                .voucherId(voucherId)
//                .build();
//        // 放入阻塞队列
//        orderTasks.add(voucherOrder);
//
//        //3. 获取代理对象（事务）
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//
//        //4. 返回订单id
//        return Result.ok(orderId);
//    }

//    public Result seckillVoucher(Long voucherId){
//        //1.查询优惠价
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        //2.判断秒杀时间
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())){
//            return Result.fail("秒杀尚未开始");
//        }
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())){
//            return Result.fail("秒杀已经结束");
//        }
//        //3.判断库存充足
//        if (voucher.getStock() < 1) {
//            return Result.fail("库存不足！");
//        }
//
//        Long userId = UserHolder.getUser().getId();
//
//        //创建锁对象
//        //SimpleRedisLock lock = new SimpleRedisLock(stringRedisTemplate, "order:" + userId);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        boolean isLock = lock.tryLock();
//        if (!isLock){
//            return Result.fail("不允许重复下单！");
//        }
//        try {
//            //获取代理对象（事务）
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } finally {
//            lock.unlock();
//        }
//    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 扣减库存
        seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0)
                .update();
        save(voucherOrder);
    }
}
