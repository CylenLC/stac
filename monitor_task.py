import requests
import time
import sys
from tqdm import tqdm

def format_size(bytes):
    """Formats bytes to human readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes < 1024:
            return f"{bytes:.2f} {unit}"
        bytes /= 1024
    return f"{bytes:.2f} PB"

def monitor_task(task_id, base_url="http://localhost:8000"):
    print(f"📡 正在连接 STAC API 监控任务: {task_id}")
    
    url = f"{base_url}/stac/tasks/{task_id}"
    pbar = None
    last_progress = 0
    
    try:
        while True:
            response = requests.get(url)
            if response.status_code != 200:
                print(f"❌ 错误: 无法获取任务状态 ({response.status_code})")
                break
                
            data = response.json()
            status = data.get("status")
            progress = data.get("progress", 0)
            message = data.get("message", "")
            eta = data.get("remaining_time")
            elapsed = data.get("elapsed_time", 0)
            total = data.get("total_bytes", 0)
            downloaded = data.get("downloaded_bytes", 0)
            
            # Initialize progress bar
            if pbar is None:
                pbar = tqdm(total=100, unit="%", desc="🚀 下载进度", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]")
            
            # Update progress
            if progress > last_progress:
                pbar.update(progress - last_progress)
                last_progress = progress
            
            # Enhanced stats
            eta_str = f"{int(eta)}s" if eta is not None else "计算中..."
            size_info = f"{format_size(downloaded)} / {format_size(total)}" if total > 0 else f"{format_size(downloaded)}"
            
            # Calculate speed
            speed = downloaded / elapsed if elapsed > 0 else 0
            
            pbar.set_postfix_str(f"{status} | {size_info} | 速: {format_size(speed)}/s | 剩: {eta_str}")
            
            if status == "completed":
                pbar.n = 100
                pbar.refresh()
                pbar.close()
                print(f"\n✅ 任务完成！总大小: {format_size(downloaded)} | 耗时: {elapsed}s")
                if data.get("results"):
                    print(f"📂 文件路径:\n" + "\n".join([f"  - {r}" for r in data["results"][:5]]))
                break
            
            if status == "failed":
                if pbar: pbar.close()
                print(f"\n❌ 任务失败: {message}")
                break
                
            time.sleep(1)
            
    except KeyboardInterrupt:
        if pbar: pbar.close()
        print("\n👋 已停止监控（任务仍会在后台继续运行）。")
    except Exception as e:
        if pbar: pbar.close()
        print(f"\n网络异常: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("使用方法: python monitor_task.py <task_id>")
    else:
        monitor_task(sys.argv[1])
