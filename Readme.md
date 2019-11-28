# Flink实时处理例程

HotItemAnalysis 模块 ： 实时热门商品统计，利用滑动窗口，eventTime

NetWorkTrafficAnalysis 模块，实时流量统计，和上面模块类似，利用滑动窗口，eventTime

LoginFailedAlarm 模块，恶意登录监控，原理同上两个模块，当检测到用户在指定时间内登录失败次数大于等于一个值，便警告