package com.alibaba.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;

//参考导入： import org.apache.commons.beanutils.BeanUtils;
import static org.apache.commons.beanutils.BeanUtils.copyProperties;


/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/6/17 11:39
 */

@Data
@AllArgsConstructor  // 带参的构造器
@NoArgsConstructor //空参
public class PaymentWide {

    Long payment_id;
    String subject;
    String payment_type;
    String payment_create_time;
    String callback_time;
    Long detail_id;
    Long order_id ;
    Long sku_id;
    BigDecimal order_price ;
    Long sku_num ;
    String sku_name;
    Long province_id;
    String order_status;
    Long user_id;
    BigDecimal total_amount;
    BigDecimal activity_reduce_amount;
    BigDecimal coupon_reduce_amount;
    BigDecimal original_total_amount;
    BigDecimal feight_fee;
    BigDecimal split_feight_fee;
    BigDecimal split_activity_amount;
    BigDecimal split_coupon_amount;
    BigDecimal split_total_amount;
    String order_create_time;

    String province_name;//查询维表得到
    String province_area_code;
    String province_iso_code;
    String province_3166_2_code;
    Integer user_age ;
    String user_gender;

    Long spu_id;     //作为维度数据 要关联进来
    Long tm_id;
    Long category3_id;
    String spu_name;
    String tm_name;
    String category3_name;

    public PaymentWide(PaymentInfo paymentInfo, OrderWide orderWide){
        mergeOrderWide(orderWide);
        mergePaymentInfo(paymentInfo);
    }

    public void  mergePaymentInfo(PaymentInfo paymentInfo)  {
        if (paymentInfo != null) {
            try {
                copyProperties(this,paymentInfo);
                payment_create_time=paymentInfo.create_time;
                payment_id = paymentInfo.id;
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
        }
    }

    public void  mergeOrderWide(OrderWide orderWide)  {
        if (orderWide != null) {
            try {
                copyProperties(this,orderWide);
                order_create_time=orderWide.create_time;
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
        }
    }

}
