import os
import time
import logging
import threading
import random
from concurrent.futures import ThreadPoolExecutor, Future, wait, FIRST_COMPLETED

import requests
from tqdm import tqdm

# ========== 配置区域（请根据网络状况调整）==========
BASE_URL_TEMPLATE = "https://dl1.wenku8.com/txtutf8/{}/{}.txt"
DOWNLOAD_DIR = r"C:\Users\Downloads\bunko8-neko"       #配置下载目录

# ---------- 并发控制 ----------
# 要求2：并行下载2个文件。若频繁出现403，建议改为1，并增大请求间隔
MAX_WORKERS = 2          # 总线程池大小 = 并行文件数

# ---------- 重试策略（要求4）----------
RETRY_TIMES = 5          # 最多重试5次
INITIAL_DELAY = 5        # 指数退避初始延迟（秒）
MAX_DELAY = 60           # 最大退避延迟（秒）

# ---------- 反反爬关键参数 ----------
# 每个请求前的随机延迟，模仿人类浏览（单位：秒）
REQUEST_DELAY = (1.5, 3.5)   # 范围下限/上限，建议不低于1.5秒
# 完整浏览器请求头，极力降低被识别为脚本的概率
DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0',
    'Referer': 'https://www.wenku8.com/',      # 来源页，部分站点校验
}
# 请求超时（连接超时，读取超时）
TIMEOUT = (10, 30)
# ================================================

# 日志配置（同时输出控制台和文件）
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
log_file = os.path.join(DOWNLOAD_DIR, "download.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# 全局变量
executor = None          # 线程池（在 main 中创建）
global_pbar = None       # 全局进度条

def download_file(session, dir_num, file_id, save_dir, attempt, pending_set, lock, pending_event):
    """
    下载单个文件，支持：
    - 浏览器请求头
    - 随机请求延时
    - 断点续传 + 文件大小检查
    - 404 跳过且不重试
    - 非阻塞指数退避重试
    - 重试任务也受线程池并发限制
    """
    url = BASE_URL_TEMPLATE.format(dir_num, file_id)
    save_path = os.path.join(save_dir, f"{file_id}.txt")

    # ---------- 断点续传：文件已存在且大小 > 0 ----------
    if os.path.exists(save_path):
        if os.path.getsize(save_path) > 0:
            logging.debug(f"跳过已存在文件: {save_path}")
            global_pbar.update(1)
            return True
        else:
            logging.warning(f"存在空文件，重新下载: {save_path}")
            os.remove(save_path)

    # ---------- 随机延迟，防止请求过快 ----------
    delay = random.uniform(*REQUEST_DELAY)
    time.sleep(delay)

    try:
        resp = session.get(url, timeout=TIMEOUT)
        if resp.status_code == 200:
            with open(save_path, "wb") as f:
                f.write(resp.content)
            logging.debug(f"下载成功: {url} -> {save_path}")
            global_pbar.update(1)
            return True
        elif resp.status_code == 404:
            logging.info(f"文件不存在 (404): {url}")
            global_pbar.update(1)
            return False
        else:
            logging.warning(f"HTTP {resp.status_code} - {url}")
    except Exception as e:
        logging.warning(f"下载异常 (尝试 {attempt}/{RETRY_TIMES}): {url} - {e}")

    # ---------- 重试逻辑（非阻塞、指数退避）----------
    if attempt < RETRY_TIMES:
        delay = min(INITIAL_DELAY * (2 ** (attempt - 1)), MAX_DELAY)
        logging.info(f"将在 {delay:.1f} 秒后重试 (第 {attempt + 1} 次): {url}")

        def retry_task():
            """延迟后提交重试任务（受同一线程池管理）"""
            fut = executor.submit(
                download_file,
                session,
                dir_num,
                file_id,
                save_dir,
                attempt + 1,
                pending_set,
                lock,
                pending_event
            )
            with lock:
                pending_set.add(fut)
            fut.add_done_callback(
                lambda f: future_done_callback(f, pending_set, lock, pending_event)
            )

        timer = threading.Timer(delay, retry_task)
        timer.daemon = True
        timer.start()
    else:
        logging.error(f"下载失败，已达最大重试次数: {url}")
        global_pbar.update(1)
        return False

def future_done_callback(fut, pending_set, lock, pending_event):
    """Future 完成时的回调：从 pending 集合移除，若集合为空则触发事件"""
    with lock:
        pending_set.discard(fut)
        if not pending_set:
            pending_event.set()

def download_directory(dir_num, id_start, id_end):
    """
    下载一个目录下的所有文件
    - 严格顺序：等待本目录所有任务（包括延迟重试）完成后才返回
    - 并发控制由线程池实现
    """
    dir_save_path = os.path.join(DOWNLOAD_DIR, str(dir_num))
    os.makedirs(dir_save_path, exist_ok=True)

    file_ids = range(id_start, id_end + 1)
    total_files = len(file_ids)
    logging.info(f"开始下载目录 {dir_num}: {id_start}~{id_end}，共 {total_files} 个文件")

    # 跟踪当前目录所有未完成的 Future（初始任务 + 重试任务）
    pending_set = set()
    lock = threading.Lock()
    pending_event = threading.Event()

    # 每个目录使用独立的 Session，自动管理Cookie和连接池
    with requests.Session() as session:
        # 设置全局请求头
        session.headers.update(DEFAULT_HEADERS)

        # 提交所有初始下载任务
        for fid in file_ids:
            fut = executor.submit(
                download_file,
                session,
                dir_num,
                fid,
                dir_save_path,
                1,  # 首次尝试
                pending_set,
                lock,
                pending_event
            )
            with lock:
                pending_set.add(fut)
            fut.add_done_callback(
                lambda f: future_done_callback(f, pending_set, lock, pending_event)
            )

        # 等待当前目录所有任务完成（包括已提交的延迟重试）
        while True:
            with lock:
                if not pending_set:
                    break
            # 使用事件等待，避免CPU忙等
            pending_event.wait(timeout=1)
            pending_event.clear()

    logging.info(f"目录 {dir_num} 所有任务完成")

def main():
    global executor, global_pbar

    # 目录配置（严格按顺序 0→1→2→3→4）
    dirs = [
        (0, 1, 999),
        (1, 1000, 1999),
        (2, 2000, 2999),
        (3, 3000, 3999),
        (4, 4000, 4999)
    ]

    total_files = sum(end - start + 1 for _, start, end in dirs)
    logging.info(f"共计 {total_files} 个文件，保存至: {DOWNLOAD_DIR}")
    logging.info(f"并发线程数: {MAX_WORKERS}, 请求间隔: {REQUEST_DELAY}秒, 重试策略: {RETRY_TIMES}次, 退避初始{INITIAL_DELAY}秒")

    # 全局进度条
    with tqdm(total=total_files, desc="总体进度", unit="file") as pbar:
        global_pbar = pbar

        # 创建全局线程池（大小由 MAX_WORKERS 决定）
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exec:
            executor = exec
            try:
                for dir_num, start, end in dirs:
                    download_directory(dir_num, start, end)
            except KeyboardInterrupt:
                logging.warning("用户中断，等待当前任务完成后退出...")
                # 不强制退出，让线程池自然完成已提交的任务

    logging.info("全部任务结束！")

if __name__ == "__main__":
    # 输出提示：若频繁403，建议调整参数
    print("=" * 60)
    print("脚本已启动。如遇403错误，请尝试：")
    print("1. 将 MAX_WORKERS 改为 1")
    print("2. 增大 REQUEST_DELAY，例如 (3, 6)")
    print("3. 增大 INITIAL_DELAY 和 MAX_DELAY")
    print("4. 更换网络IP（重启路由器或使用代理）")
    print("=" * 60)
    main()