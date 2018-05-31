package com.bjtuling.utils

import java.text.{ParseException, ParsePosition, SimpleDateFormat}
import java.time.format.DateTimeFormatter
import java.util.{Calendar, Date, TimeZone}

import com.google.common.base.Strings
import org.joda.time.format.DateTimeFormat
import org.joda.time.{LocalDate, Months}

/**
  * 日期工具类
  * Created by KG on 2017/4/8.
  */
object DateUtil {
  private val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
  private val datetimeFormatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  private val dateFormatterYYYYMMDD = new SimpleDateFormat("yyyyMMdd")
  private val datetimeFormatterYYYYMMDDHHMMSS = new SimpleDateFormat("yyyyMMddHHmmss")
  private val defaultTimeZone: TimeZone = TimeZone.getTimeZone("GMT+08:00")

  /**
    * 获取指定时间的昨天日期（格式：2016-10-13）
    *
    * @param startDate 指定时间
    * @return
    */
  def getYesterdayByJob(startDate: String): String = {

    val cal: Calendar = Calendar.getInstance()
    cal.setTime(dateFormatter.parse(startDate))

    cal.add(Calendar.DATE, -1)
    val yesterday = dateFormatter.format(cal.getTime())
    yesterday
  }

  /**
    * 获取指定时间的上月日期（格式：2016-10-13）
    *
    * @param startDate 指定时间
    * @return
    */
  def getLastMonthByJob(startDate: String): String = {

    val cal: Calendar = Calendar.getInstance()
    cal.setTime(dateFormatter.parse(startDate))

    cal.add(Calendar.MONTH, -1)
    val yesterday = dateFormatter.format(cal.getTime())
    yesterday
  }

  /**
    * 增加一天
    *
    * @param startDate 指定时间
    * @return
    */
  def addOneDay(startDate: String): String = {

    val cal: Calendar = Calendar.getInstance()
    cal.setTime(dateFormatter.parse(startDate))
    cal.add(Calendar.DATE, 1)
    val one = dateFormatter.format(cal.getTime())
    one
  }

  /**
    * 增加一天
    *
    * @param startDate 指定时间
    * @return
    */
  def addOneMonth(startDate: String): String = {

    val cal: Calendar = Calendar.getInstance()
    cal.setTime(dateFormatter.parse(startDate))
    cal.add(Calendar.MONTH, 1)
    val one = dateFormatter.format(cal.getTime())
    one
  }

  /**
    * 时间做加减法
    *
    * @param dt   指定时间
    * @param days 天数，可以为为负数
    * @return
    */
  def minusDays(dt: String, days: Int): String = {
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(dateFormatter.parse(dt))
    cal.add(Calendar.DATE, days)
    val one = dateFormatter.format(cal.getTime())
    one
  }

  /**
    * 获取当前系统时间的昨天日期（格式：2016-10-13）
    *
    * @return
    */
  def getYesterday(): String = {
    val cal: Calendar = Calendar.getInstance(defaultTimeZone)
    cal.add(Calendar.DATE, -1)
    dateFormatter.format(cal.getTime())
  }

  /**
    * 获取当前系统时间的昨天日期（格式：2016-10-13）
    *
    * @return
    */
  def getYesterdayYYYYMMDD(): String = {
    val cal: Calendar = Calendar.getInstance(defaultTimeZone)
    cal.add(Calendar.DATE, -1)
    dateFormatterYYYYMMDD.format(cal.getTime())
  }

  /**
    * 获取开始日期和结束日期质之间的间隔天数
    *
    * @param startDate
    * @param endDate
    * @return
    */
  def getIntervalDays(startDate: String, endDate: String): Long = {
    val days = (dateFormatter.parse(endDate).getTime - dateFormatter.parse(startDate).getTime) / (1000 * 3600 * 24)
    days
  }

  /**
    * 获取当前系统时间，格式为：yyyy-MM-dd HH:mm:ss
    *
    * @return
    */
  def getCurrentSystemTime(): String = {
    val now: Date = new Date()
    datetimeFormatter.format(now)
  }

  /**
    * 是否全量更新
    *
    * @param startDate
    * @param endDate
    * @return
    */
  def isFullUpdate(startDate: String, endDate: String): Boolean = {
    if (!Strings.isNullOrEmpty(startDate) && !Strings.isNullOrEmpty(endDate)) {
      false
    } else {
      true
    }
  }

  /**
    * 获取当前日期，格式为：yyyy-MM-dd
    *
    * @return
    */
  def getCurrentDate(): String = {
    val now: Date = new Date()
    dateFormatter.format(now)
  }

  /**
    * 获取当前日期，格式为：yyyyMMdd
    *
    * @return
    */
  def getCurrentDateYYYYMMDD(): String = {
    val now: Calendar = Calendar.getInstance(defaultTimeZone)
    dateFormatterYYYYMMDD.format(now.getTime)
  }

  /**
    * 计算方法运行时间开销
    *
    * @param actionName 传入当前JOB名称
    * @param action     待测试代码
    * @tparam U
    * @return
    */
  def timeCost[U](actionName: String)(action: => U): Unit = {
    val start = System.currentTimeMillis()
    val res = action
    val ss: Int = ((System.currentTimeMillis() - start) / 1000).toInt
    val MM: Int = ss / 60
    val hh: Int = MM / 60
    val dd: Int = hh / 24
    println(s"#### $actionName TimeCost: " + dd + "d " + (hh - dd * 24) + "h " + (MM - hh * 60) + "m " + (ss - MM * 60) + "s", "Total：" + (System.currentTimeMillis() - start) + "ms ")
  }

  def parseDate(str: String, parsePatterns: String): Date = {
    if (str == null) return null
    try {
      val formatter: SimpleDateFormat = new SimpleDateFormat(parsePatterns);
      val pos: ParsePosition = new ParsePosition(0);
      val strtodate: Date = formatter.parse(str, pos);
      strtodate
    } catch {
      case e: ParseException => {
        null
      }
    }
  }

  def getDaysBetween(start: String, end: String): Seq[String] = {
    var seq = Seq[String]()
    val startDate: java.time.LocalDate = java.time.LocalDate.from(DateTimeFormatter.ofPattern("yyyy-MM-dd").parse(start))
    val endDate: java.time.LocalDate = java.time.LocalDate.from(DateTimeFormatter.ofPattern("yyyy-MM-dd").parse(end))
    val days: Long = endDate.toEpochDay - startDate.toEpochDay
    for (i <- 0 until days.toInt) {
      seq = seq :+ startDate.plusDays(i).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    }
    seq
  }

}
