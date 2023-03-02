import functools
import os
import shutil
import sys
import time
from concurrent.futures import Future, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Iterable, List, Optional, Callable, Any

num_done_files = 0
num_done_dirs = 0
num_done_bytes = 0


def pretty_size(size: int) -> str:
    """ pretty print file size """
    if size < 1024:
        return f"{size} B"
    if size < 1024 * 1024:
        return f"{size / 1024:.2f} KiB"
    if size < 1024 * 1024 * 1024:
        return f"{size / 1024 / 1024:.2f} MiB"
    return f"{size / 1024 / 1024 / 1024:.2f} GiB"


start_time = 0
last_print_time = 0


def print_progress_inplace(path: Optional[Path] = None, force=False) -> None:
    """ print a path in place of the previous line """
    global last_print_time
    current_time = time.time()
    if not force and current_time - last_print_time < 0.1:
        return
    last_print_time = current_time

    if path is None:
        path_text = ""
    else:
        path_text = str(path)
    items_per_second = (num_done_files + num_done_dirs) / (
            current_time - start_time + 1e-6)
    prefix = f'{num_done_files} files, {num_done_dirs} dirs, ' \
             f'total size: {pretty_size(num_done_bytes)}, ' \
             f'{items_per_second:.2f} items/s'

    terminal_width = shutil.get_terminal_size().columns - 1
    if len(prefix) + len(path_text) > terminal_width:
        path_text = "..." + path_text[-terminal_width + len(prefix) + 5:]
    if path_text:
        print(f"\r{prefix}|{path_text}".ljust(terminal_width), end="")
    else:
        print(f"\r{prefix}".ljust(terminal_width), end="")


def func_wrapper(func: Callable[[Path, Path], None], is_dir, path: Path,
                 root: Path) -> None:
    """ wrapper to catch exceptions and print progress """
    global num_done_files, num_done_dirs, num_done_bytes
    size = path.stat().st_size
    try:
        func(path, root)
    except:  # noqa
        import traceback
        print(traceback.format_exc())
        # abort entire process
        os._exit(1)  # noqa
    if is_dir:
        num_done_dirs += 1
    else:
        num_done_files += 1
    num_done_bytes += size


def after_futures_done(futures: Iterable[Future],
                       func: Callable[[], None]) -> None:
    """ apply func after all futures are done """
    wait(futures)
    func()


def parallel_recursive_apply(paths: List[str],
                             dir_func: Callable[[Path, Path], Any],
                             file_func: Callable[[Path, Path], Any],
                             pre_order: bool = True,
                             num_max_threads: int = 512) -> None:
    """
    Apply dir_func and file_func to all files and directories in paths

    :param paths: List of paths to visit
    :param dir_func: (path, root_path) -> Any. Function to apply to a directory
    :param file_func: (path, root_path) -> Any. Function to apply to a file
    :param pre_order: If True, apply dir_func before applying file_func
        to the files in a directory. If False, apply file_func before
        applying dir_func to the directories in a directory.
    :param num_max_threads: Maximum number of threads to use
        Note that you should balance the time spent in dir_func and
        file_func with the number of threads, because the more threads
        you use, the more time is spent in thread management.
    """
    global start_time
    start_time = time.time()

    dir_func = functools.partial(func_wrapper, dir_func, True)
    file_func = functools.partial(func_wrapper, file_func, False)
    with ThreadPoolExecutor(max_workers=num_max_threads) as executor:
        for path in paths:
            if not os.path.exists(path):
                raise FileNotFoundError(path)
            if not os.path.isdir(path):
                executor.submit(file_func, Path(path))
                continue
            if pre_order:
                _parallel_pre_order_apply(path,
                                          dir_func, file_func,  # type: ignore
                                          executor)
            else:
                _parallel_post_order_apply(path,
                                           dir_func, file_func,  # type: ignore
                                           executor)

    print_progress_inplace(force=True)


def _parallel_pre_order_apply(
        root_dir: str,
        dir_func: Callable[[Path, Path], Any],
        file_func: Callable[[Path, Path], Any],
        executor: ThreadPoolExecutor) -> None:
    """ pre-order apply """
    root_dir = Path(root_dir)
    parent_future = executor.submit(dir_func, root_dir, root_dir)
    parent_future.add_done_callback(
        lambda _: print_progress_inplace(root_dir))
    dirpath_to_future = {root_dir: parent_future}

    for parent, dirnames, filenames in os.walk(root_dir):
        parent = Path(parent)
        parent_future = dirpath_to_future.pop(parent)
        for dirname in dirnames:
            dirpath = parent / dirname
            future = executor.submit(
                after_futures_done, [parent_future],
                lambda path=dirpath: dir_func(path, root_dir))
            future.add_done_callback(
                lambda _, path=dirpath: print_progress_inplace(path))
            dirpath_to_future[dirpath] = future
        for filename in filenames:
            filepath = parent / filename
            future = executor.submit(
                after_futures_done, [parent_future],
                lambda path=filepath: file_func(path, root_dir))
            future.add_done_callback(
                lambda _, path=filepath: print_progress_inplace(path))


def _parallel_post_order_apply(
        root_dir: str,
        dir_func: Callable[[Path, Path], Any],
        file_func: Callable[[Path, Path], Any],
        executor: ThreadPoolExecutor) -> None:
    """ post-order apply """
    root_dir = Path(root_dir)
    dirpath_to_future = {}
    for parent, dirnames, filenames in os.walk(root_dir, topdown=False):
        parent = Path(parent)
        child_futures = []
        for dirname in dirnames:
            child_futures.append(dirpath_to_future.pop(parent / dirname))

        for filename in filenames:
            future = executor.submit(file_func, parent / filename, root_dir)
            future.add_done_callback(
                lambda _, path=parent: print_progress_inplace(path))
            child_futures.append(future)

        future = executor.submit(after_futures_done, child_futures,
                                 lambda path=parent: dir_func(path, root_dir))
        future.add_done_callback(
            lambda _, path=parent: print_progress_inplace(path))
        dirpath_to_future[parent] = future
