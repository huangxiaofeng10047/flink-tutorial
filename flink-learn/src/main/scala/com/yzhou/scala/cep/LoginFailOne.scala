package com.yzhou.scala.cep

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.util
import scala.collection.mutable.ListBuffer


/*
  对于网站而言，用户登录并不是频繁的业务操作。如果一个用户短时间内频繁登录失败，就有可能是出现了程序的恶意攻击，比如密码暴力破解。因此我们考虑，应该对用户的登录失败动作进行统计，具体来说，如果同一用户（可以是不同IP）在2秒之内连续两次登录失败，就认为存在恶意登录的风险，输出相关的信息进行报警提示。这是电商网站、也是几乎所有网站风控的基本一环。
  所以我们可以思考一下解决方案：
  基本需求 -- 用户在短时间内频繁登录失败，有程序恶意攻击的可能 -- 同一用户（可以是不同IP）在2秒内连续两次登录失败，需要报警
  解决思路 -- 将用户的登录失败行为存入 ListState，设定定时器2秒后触发，查看 ListState 中有几次失败登录 -- 更加准确的检测，可以使用 CEP 库实现事件流的模式匹配
  既然现在思路清楚了，那我们就尝试将方案落地。
 */
object LoginFailOne {

  // 输入的登录事件样例类
  case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)
  // 输出的报警信息样例类
  case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

  def main(args: Array[String]): Unit = {

    // 创建流环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度
    env.setParallelism(1)
    // 设置时间特征为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取csv文件
    env.readTextFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/files/LoginLog.csv")
      .map(data => {
        // 将文件中的数据封装成样例类
        val dataArray: Array[String] = data.split(",")
        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      // 设置 WaterMark 水印
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000
      })
      // 以用户id为key，进行分组
      .keyBy(_.userId)
      // 计算出同一个用户2秒内连续登录失败超过2次的报警信息
      .process(new LoginWarning(2))
      .print()

    //  执行程序
    env.execute("login fail job")


  }

  // 自定义处理函数，保留上一次登录失败的事件，并可以注册定时器    [键的类型，输入元素的类型，输出元素的类型]
  class LoginWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {

    // 定义  保存登录失败事件的状态
    lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginfail-state", classOf[LoginEvent]))

    override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {

      // 判断当前登录状态是否为 fail
      if (value.eventType == "fail") {
        // 判断存放失败事件的state是否有值，没有值则创建一个2秒后的定时器
        if (!loginFailState.get().iterator().hasNext) {
          // 注册一个定时器，设置在 2秒 之后
          ctx.timerService().registerEventTimeTimer((value.eventTime + 2) * 1000L)
        }
        // 把新的失败事件添加到  state
        loginFailState.add(value)
      } else {
        // 如果登录成功，清空状态重新开始
        loginFailState.clear()
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
      // 触发定时器的时候，根据状态的失败个数决定是否输出报警
      val allLoginFailEvents: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]()

      val iter: util.Iterator[LoginEvent] = loginFailState.get().iterator()

      // 遍历状态中的数据，将数据存放至 ListBuffer
      while (iter.hasNext) {
        allLoginFailEvents += iter.next()
      }

      //判断登录失败事件个数，如果大于等于 maxFailTimes ，输出报警信息
      if (allLoginFailEvents.length >= maxFailTimes) {
        out.collect(Warning(allLoginFailEvents.head.userId,
          allLoginFailEvents.head.eventTime,
          allLoginFailEvents.last.eventTime,
          "在2秒之内连续登录失败" + allLoginFailEvents.length + "次"))
      }

      // 清空状态
      loginFailState.clear()
    }
  }
}