import argparse
import functools
import getpass
import glob
import os
import re
import sys
from pathlib import Path
from queue import Queue

import paramiko

from parallel_traversal import parallel_recursive_apply


def scp_dir(src: Path, src_root: Path, dest_root: Path, as_child: bool,
            connections: Queue) -> None:
    """ scp src dir to the corresponding location in dest_root """
    dest = dest_root / src.relative_to(src_root)
    if as_child:
        dest = dest.parent
    # assume that the parent directory of dest exists
    sftp = connections.get()
    try:
        sftp.mkdir(str(dest), mode=src.stat().st_mode)
    except OSError:
        import traceback
        print(f'\r{traceback.format_exc()}')
    connections.put(sftp)


def scp_file(src: Path, src_root: Path, dest_root: Path, as_child: bool,
             connections: Queue) -> None:
    """ scp src file to the corresponding location in dest_root """
    dest = dest_root / src.relative_to(src_root)
    if as_child:
        dest = dest.parent
    # assume that the parent directory of dest exists
    sftp = connections.get()
    try:
        sftp.put(str(src), str(dest), confirm=False)
    except OSError:
        import traceback
        print(f'\r{traceback.format_exc()}')
    connections.put(sftp)


def parallel_scp_r(dest_host, dest_port, dest_user, identity_filename,
                   source_paths, dest_path, num_max_threads):
    # Try authentication
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    authentication = dict()
    # Try key files
    keys = []
    if identity_filename is not None:
        keys.append(identity_filename)
    ssh_dir = os.path.expanduser('~/.ssh')
    if os.path.isdir(ssh_dir):
        for f in os.listdir(ssh_dir):
            if f.endswith('.pub'):
                # Skip public keys
                continue
            keys.append(os.path.join(ssh_dir, f))
    for key in keys:
        try:
            client.connect(dest_host, port=dest_port, username=dest_user,
                           key_filename=key)
            authentication['key_filename'] = key
            break
        except paramiko.ssh_exception.AuthenticationException:
            pass
    # Try password
    for _ in range(3):
        try:
            password = getpass.getpass(f'{dest_user}@{dest_host}\'s password: ')
            client.connect(dest_host, port=dest_port, username=dest_user,
                           password=password)
            authentication['password'] = password
            break
        except paramiko.ssh_exception.AuthenticationException:
            pass
    else:
        raise paramiko.ssh_exception.AuthenticationException(
            'Authentication failed.')
    # Make connection pool
    connections = Queue(maxsize=num_max_threads)
    connections.put(client.open_sftp())
    print(f'Establishing {num_max_threads} connections.')
    for i in range(num_max_threads - 1):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(dest_host, port=dest_port, username=dest_user,
                       **authentication)
        connections.put(client.open_sftp())
        print(f'\r{i + 1}/{num_max_threads} connections established.',
              end='', flush=True)
    print()
    # Copy
    parallel_recursive_apply(
        source_paths,
        dir_func=functools.partial(  # type: ignore
            scp_dir, dest_root=Path(dest_path),
            as_child=True, connections=connections),
        file_func=functools.partial(  # type: ignore
            scp_file, dest_root=Path(dest_path),
            as_child=True, connections=connections),
        pre_order=True,
        num_max_threads=num_max_threads)
    # Close connections
    for _ in range(num_max_threads):
        connections.get().close()


def main():
    parser = argparse.ArgumentParser(
        description='Parallel scp -r with paramiko')
    parser.add_argument('local_src', type=str, help='local source paths', nargs='+')
    parser.add_argument('remote_dest', type=str,
                         help='remote destination path in the form of '
                                'user@host:path')
    parser.add_argument('-t', '--threads', type=int, default=64,
                        help='number of threads')
    parser.add_argument('-P', '--port', type=int, default=22,
                        help='port number')
    parser.add_argument('-i', '--identity', type=str, default=None,
                        help='identity file')
    args = parser.parse_args()

    # remote_dest -> user@host:path
    m = re.match(r'((?P<user>[^@]+)@)?(?P<host>[^:]+):(?P<path>.*$)',
                 args.remote_dest)
    if m is None:
        raise ValueError(f'Invalid destination: {args.remote_dest}')
    dest_user = m.group('user') or os.getlogin()
    dest_host = m.group('host')
    dest_path = m.group('path')

    # src -> paths
    paths = []
    # noinspection DuplicatedCode
    for pattern in args.local_src:
        # noinspection DuplicatedCode
        if sys.version_info >= (3, 11):
            matches = glob.glob(pattern, recursive=True, include_hidden=True)
        else:
            matches = glob.glob(pattern, recursive=True)
        if not matches:
            # if pattern does not match anything, skip it
            print(f"Warning: Skipped {pattern}: Does not match "
                  "any file or directory.")
        paths.extend(matches)

    parallel_scp_r(dest_host, args.port, dest_user, args.identity, paths,
                   dest_path, args.threads)


if __name__ == '__main__':
    main()
