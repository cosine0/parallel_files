import enum
import functools
import os
import shutil
import stat
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Iterable, List, Optional, Callable, Any


def walk_post_order(top):
    dirs = []
    nondirs = []
    walk_dirs = []

    # We may not have read permission for top, in which case we can't
    # get a list of the files the directory contains.  os.walk
    # always suppressed the exception then, rather than blow up for a
    # minor reason when (say) a thousand readable directories are still
    # left to visit.  That logic is copied here.
    scandir_it = os.scandir(top)

    with scandir_it:
        while True:
            try:
                entry = next(scandir_it)
            except StopIteration:
                break
            try:
                is_dir = entry.is_dir()
            except OSError:
                # If is_dir() raises an OSError, consider that the entry is not
                # a directory, same behaviour than os.path.isdir().
                is_dir = False

            if is_dir:
                dirs.append(entry.name)
            else:
                nondirs.append(entry.name)

            if is_dir:
                # Bottom-up: recurse into sub-directory, but exclude symlinks to
                # directories if followlinks is False
                try:
                    os.readlink(entry.path)
                    is_symlink = True
                except OSError:
                    # If os.readlink() raises an OSError, consider that the
                    # entry is not a symbolic link.
                    is_symlink = False
                walk_into = not is_symlink

                if walk_into:
                    walk_dirs.append(entry.path)

    # Yield before recursion if going top down
    # Recurse into sub-directories
    for new_path in walk_dirs:
        yield from walk_post_order(new_path)
    # Yield after recursion going bottom up
    yield top, dirs, nondirs


num_done_files = 0
num_done_dirs = 0
num_done_bytes = 0


class FileType(enum.Enum):
    """ file type """
    NONEXISTENT = enum.auto()
    FILE = enum.auto()
    DIRECTORY = enum.auto()
    SYMLINK = enum.auto()
    JUNCTION = enum.auto()
    DEVICE = enum.auto()
    UNKNOWN = enum.auto()

    @staticmethod
    def from_path(path: Path) -> 'FileType':
        """ get file type from path """
        try:
            mode = path.lstat().st_mode
        except FileNotFoundError:
            return FileType.NONEXISTENT
        if stat.S_ISLNK(mode):
            return FileType.SYMLINK
        try:
            os.readlink(path)
            return FileType.JUNCTION
        except OSError:
            pass
        if stat.S_ISDIR(mode):
            return FileType.DIRECTORY
        if stat.S_ISREG(mode):
            return FileType.FILE
        if stat.S_ISBLK(mode) or stat.S_ISCHR(mode) or stat.S_ISFIFO(mode) or \
            stat.S_ISSOCK(mode):
            return FileType.DEVICE
        return FileType.UNKNOWN


def pretty_size(size: float) -> str:
    """ pretty print file size """
    if size < 1024:
        return f"{size} B"
    if size < 1024 * 1024:
        return f"{size / 1024:.2f} KiB"
    if size < 1024 * 1024 * 1024:
        return f"{size / 1024 / 1024:.2f} MiB"
    return f"{size / 1024 / 1024 / 1024:.2f} GiB"


def pretty_time(seconds: float) -> str:
    """ pretty print time """
    if seconds < 60:
        return f"{seconds:.2f} s"
    if seconds < 60 * 60:
        return f"{seconds / 60:2.0f}:{seconds % 60:02.0f}"
    if seconds < 60 * 60 * 24:
        return f"{seconds / 60 / 60:2.0f}:{seconds % 60 / 60:02.0f}:" \
               f"{seconds % 60 % 60:02.0f}"


start_time = 0
last_print_time = 0


def print_progress_inplace(path: Optional[Path] = None,
                           print_lock: Optional[threading.Lock] = None,
                           keep_interval=True) -> None:
    """ print a path in place of the previous line """
    global last_print_time
    current_time = time.time()
    if keep_interval and current_time - last_print_time < 0.1:
        return
    last_print_time = current_time

    if path is None:
        path_text = ""
    else:
        path_text = str(path)
    items_per_second = (num_done_files + num_done_dirs) / (
        current_time - start_time + 1e-6)
    bytes_per_second = num_done_bytes / (current_time - start_time + 1e-6)
    message = f'{num_done_files} files, {num_done_dirs} dirs, ' \
              f'total size: {pretty_size(num_done_bytes)}, ' \
              f'{items_per_second:.2f} items/s, ' \
              f'{pretty_size(bytes_per_second)}/s, ' \
              f'elapsed: {pretty_time(current_time - start_time)}'
    if path_text:
        message += f", current: "

    terminal_width = shutil.get_terminal_size().columns - 1
    if len(message) > terminal_width:
        message = message[:terminal_width]
    elif len(message) + len(path_text) > terminal_width:
        # ellipsis the middle of the path
        path_text = path_text[:(terminal_width - len(message) - 3) // 2] + \
                    "..." + \
                    path_text[-(terminal_width - len(message) - 3) // 2:]
    message += path_text
    message = message.ljust(terminal_width)

    if print_lock is not None:
        with print_lock:
            print(f"\r{message}", end="")
    else:
        print(f"\r{message}", end="")


def call_with_progress_and_try(
    func: Callable[[Path, Path], None],
    path: Path,
    root: Path,
    is_dir: bool,
    print_lock: Optional[threading.Lock] = None
) -> None:
    """ wrapper to catch exceptions and print progress """
    global num_done_files, num_done_dirs, num_done_bytes
    size = path.stat(follow_symlinks=False).st_size
    try:
        func(path, root)
    except:  # noqa
        import traceback
        if print_lock is not None:
            with print_lock:
                print(f"\rError: {path}\n{traceback.format_exc()}")
        else:
            print(f"\rError: {path}\n{traceback.format_exc()}")
        # abort entire process
        os._exit(1)  # noqa
    if is_dir:
        num_done_dirs += 1
    else:
        num_done_files += 1
    num_done_bytes += size
    if print_lock is not None:
        with print_lock:
            print_progress_inplace(path)
    else:
        print_progress_inplace(path)


def after_futures_done(futures: Iterable[Future],
                       func: Callable[[], None]) -> None:
    """ apply func after all futures are done """
    wait(futures)
    func()


def parallel_recursive_apply(
    paths: List[str],
    dir_func: Callable[[Path, Path], Any],
    file_func: Callable[[Path, Path], Any],
    pre_order: bool = True,
    num_max_threads: int = 512,
    strict_hierarchical_order: bool = True,
    print_lock: Optional[threading.Lock] = None) -> None:
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
    :param strict_hierarchical_order: If True, apply dir_func and file_func
        after their parents are done (for pre_order=True) or after
        all their children are done (for pre_order=False).
    :param print_lock: If not None, use this lock to print progress and
        error messages.
    """
    global start_time
    start_time = time.time()

    dir_func = functools.partial(call_with_progress_and_try, dir_func,
                                 is_dir=True, print_lock=print_lock)
    file_func = functools.partial(call_with_progress_and_try, file_func,
                                  is_dir=False, print_lock=print_lock)
    with ThreadPoolExecutor(max_workers=num_max_threads) as executor:
        for path in paths:
            if not os.path.isdir(path):
                executor.submit(file_func, Path(path))
                continue
            if pre_order:
                _parallel_pre_order_apply(path,
                                          dir_func, file_func,  # type: ignore
                                          executor, strict_hierarchical_order)
            else:
                _parallel_post_order_apply(path,
                                           dir_func, file_func,  # type: ignore
                                           executor, strict_hierarchical_order)

    print_progress_inplace(keep_interval=False)
    print()


def _parallel_pre_order_apply(root_dir: str,
                              dir_func: Callable[[Path, Path], Any],
                              file_func: Callable[[Path, Path], Any],
                              executor: ThreadPoolExecutor,
                              strict_hierarchical_order: bool) -> None:
    """ pre-order apply """
    root_dir = Path(root_dir)
    parent_future = executor.submit(dir_func, root_dir, root_dir)
    dirpath_to_future = {root_dir: parent_future}

    for parent, dirnames, filenames in os.walk(root_dir, followlinks=False):
        parent = Path(parent)
        parent_future = dirpath_to_future.pop(parent)
        prune_dir_indexes = []
        for i, dirname in enumerate(dirnames):
            dirpath = parent / dirname
            if FileType.from_path(dirpath) != FileType.DIRECTORY:
                # symlink, junction, etc. -> treat as file, and don't recurse
                filenames.append(dirname)
                prune_dir_indexes.append(i)
                continue
            future = executor.submit(
                after_futures_done,
                [parent_future] if strict_hierarchical_order else [],
                lambda path=dirpath: dir_func(path, root_dir))
            dirpath_to_future[dirpath] = future
        for i in reversed(prune_dir_indexes):
            del dirnames[i]
        for filename in filenames:
            filepath = parent / filename
            if not os.path.lexists(filepath):
                # nonexistent -> skip
                continue
            executor.submit(
                after_futures_done,
                [parent_future] if strict_hierarchical_order else [],
                lambda path=filepath: file_func(path, root_dir))


def _parallel_post_order_apply(root_dir: str,
                               dir_func: Callable[[Path, Path], Any],
                               file_func: Callable[[Path, Path], Any],
                               executor: ThreadPoolExecutor,
                               strict_hierarchical_order: bool) -> None:
    """ post-order apply """
    root_dir = Path(root_dir)
    dirpath_to_future = {}
    for parent, dirnames, filenames in walk_post_order(root_dir):
        parent = Path(parent)
        child_futures = []
        for dirname in dirnames:
            dirpath = parent / dirname
            if FileType.from_path(dirpath) != FileType.DIRECTORY:
                filenames.append(dirname)
                continue
            child_futures.append(dirpath_to_future.pop(parent / dirname))

        for filename in filenames:
            filepath = parent / filename
            # there's some symlinks to directories in the filenames
            future = executor.submit(file_func, filepath, root_dir)
            child_futures.append(future)

        future = executor.submit(
            after_futures_done,
            child_futures if strict_hierarchical_order else [],
            lambda path=parent: dir_func(path, root_dir))
        dirpath_to_future[parent] = future
