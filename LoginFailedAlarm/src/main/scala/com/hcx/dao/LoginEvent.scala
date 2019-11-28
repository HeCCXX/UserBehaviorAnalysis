package com.hcx.dao

/**
 * 用户登录事件样例类
 * @param userId
 * @param ip
 * @param loginFlag
 * @param timestamp
 */
case class LoginEvent(userId : Long,ip : String,loginFlag : String,timestamp : Long)
