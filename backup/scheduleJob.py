from apscheduler.schedulers.blocking import BlockingScheduler
from stocklib.stock_data_init import  stockDataInit

def job():
    try:
        stock = stockDataInit(market='H')
        stock.init_stock_allmarket_by_day()
        print("任务执行完成")
    except Exception as e:
        print(f"任务执行失败: {e}")

if __name__ == '__main__':
    # 创建调度器（BlockingScheduler会阻塞当前线程）job
    job()

    scheduler = BlockingScheduler(timezone='Asia/Shanghai')  # 指定时区为北京时间

    # 添加任务：每天12点执行
    scheduler.add_job(
        job,
        'cron',
        hour=12,
        minute=0,
        second=0
    )

    print("定时任务已启动，每天12点执行...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass