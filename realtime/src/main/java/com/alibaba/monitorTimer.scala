package com.alibaba

import java.util.Date


/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/7/1 14:17
 */
object monitorTimer {
    def periodicCall(s:Integer , callback:()=>Unit)={
      while(true){
        callback()
        Thread.sleep(s*1000)
      }
    }

  def main(args: Array[String]): Unit = {
    // java.util.Date
    periodicCall(10,()=>println("hello " + new Date().getSeconds))

    /*
      定时执行：
      hello 28
      hello 38
      hello 48
     */
  }

}
