package com.alibaba;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/6/9 18:25
 */
//fixme: Demo2<T>
public class Demo2<T> {
    /**
     * 返回值，直接写T表示限制参数的类型，这种方法一般多用于共同操作一个类对象，然后获取里面的集合信息啥的。
     * @param args
     */

    public static void main(String[] args) {

        //限制T 为String 类型
        Demo2<String> demo = new Demo2<String>();

        //获取string类型
        List<String> array = new ArrayList<String>();
        array.add("test");
        array.add("doub");
        String str = demo.getListFisrt(array);
        System.out.println(str);

        //获取Integer类型 T 为Integer类型
        Demo2<Integer> demo2 = new Demo2<Integer>();
        List<Integer> nums = new ArrayList<Integer>();
        nums.add(12);
        nums.add(13);
        Integer num = demo2.getListFisrt(nums);
        System.out.println(num);
    }


    /**
     * 这个只能传递T类型的数据
     * 返回值 就是Demo<T> 实例化传递的对象类型
     * @param data
     * @return
     */
    private T getListFisrt(List<T> data) {
        if (data == null || data.size() == 0) {
            return null;
        }
        return data.get(0);
    }

}
