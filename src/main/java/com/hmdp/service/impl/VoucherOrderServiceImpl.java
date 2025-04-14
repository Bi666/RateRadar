package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Transactional
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Autowired
    private ISeckillVoucherService seckillVoucherService;
    @Autowired
    private RedisIdWorker redisIdWorker;

    public Result seckillVoucher(Long voucherId){
        //1.查询优惠价
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        //2.判断秒杀时间
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())){
            return Result.fail("秒杀尚未开始");
        }
        if (voucher.getEndTime().isBefore(LocalDateTime.now())){
            return Result.fail("秒杀已经结束");
        }
        //3.判断库存充足
        if (voucher.getStock() < 1) {
            return Result.fail("库存不足！");
        }
        //4.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .update();
        if (!success){
            return Result.fail("库存不足！");
        }
        //5.创建订单
        long orderId = redisIdWorker.nextId("order");
        Long userId = UserHolder.getUser().getId();
        VoucherOrder voucherOrder = VoucherOrder.builder()
                .id(orderId)
                .userId(userId)
                .voucherId(voucherId)
                .build();
        save(voucherOrder);
        //6.返回订单id
        return Result.ok(voucherOrder.getId());
    }
}
