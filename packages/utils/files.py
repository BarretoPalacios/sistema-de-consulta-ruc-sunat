import os
import shutil


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def safe_rename(src: str, dst: str):
    if os.path.exists(dst):
        os.remove(dst)
    os.rename(src, dst)


def get_file_size(path: str) -> int:
    return os.path.getsize(path) if os.path.exists(path) else 0


def check_disk_space(target_path: str, min_gb: float = 2.0):
    target = os.path.dirname(os.path.abspath(target_path)) if target_path else "."
    if not target:
        target = "."
    total, used, free = shutil.disk_usage(target)
    free_gb = free / (1024 ** 3)
    if free_gb < min_gb:
        raise RuntimeError(f"Espacio insuficiente: {free_gb:.1f} GB libres (mínimo {min_gb} GB)")
    return free_gb


def remove_if_exists(path: str):
    if os.path.isfile(path):
        os.remove(path)
    elif os.path.isdir(path):
        shutil.rmtree(path)
