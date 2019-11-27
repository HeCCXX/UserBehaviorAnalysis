package com.hcx.dao

/**
 * @Author HCX
 * @Description //TODO 用户行为样例类，对应csv中的数据格式
 * @Date 13:17 2019-11-27
 *
 * @return
 * @exception
 **/

case class UserBehavior(
                         userId:Long,
                         itemId:Long,
                         categoryId:Int,
                         behavior:String,
                         timestamp:Long
                       )
