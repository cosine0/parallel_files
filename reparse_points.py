import ctypes
import os
from _winapi import CreateFile, GENERIC_READ, OPEN_EXISTING, NULL, \
    INVALID_HANDLE_VALUE, CloseHandle
from ctypes import windll, wintypes
from enum import Enum
from typing import Tuple, Union

# Constants
FILE_SHARE_READ = 0x00000001
FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000
FSCTL_GET_REPARSE_POINT = 0x000900A8
MAXIMUM_REPARSE_DATA_BUFFER_SIZE = 0x4000
SYMLINK_FLAG_RELATIVE = 0x00000001


class ReparseTag(Enum):
    IO_REPARSE_TAG_RESERVED_ZERO = 0x00000000
    IO_REPARSE_TAG_RESERVED_ONE = 0x00000001
    IO_REPARSE_TAG_RESERVED_TWO = 0x00000002
    IO_REPARSE_TAG_MOUNT_POINT = 0xA0000003
    IO_REPARSE_TAG_HSM = 0xC0000004
    IO_REPARSE_TAG_DRIVE_EXTENDER = 0x80000005
    IO_REPARSE_TAG_HSM2 = 0x80000006
    IO_REPARSE_TAG_SIS = 0x80000007
    IO_REPARSE_TAG_WIM = 0x80000008
    IO_REPARSE_TAG_CSV = 0x80000009
    IO_REPARSE_TAG_DFS = 0x8000000A
    IO_REPARSE_TAG_FILTER_MANAGER = 0x8000000B
    IO_REPARSE_TAG_SYMLINK = 0xA000000C
    IO_REPARSE_TAG_IIS_CACHE = 0xA0000010
    IO_REPARSE_TAG_DFSR = 0x80000012
    IO_REPARSE_TAG_DEDUP = 0x80000013
    IO_REPARSE_TAG_APPXSTRM = 0xC0000014
    IO_REPARSE_TAG_NFS = 0x80000014
    IO_REPARSE_TAG_FILE_PLACEHOLDER = 0x80000015
    IO_REPARSE_TAG_DFM = 0x80000016
    IO_REPARSE_TAG_WOF = 0x80000017
    IO_REPARSE_TAG_WCI = 0x80000018
    IO_REPARSE_TAG_WCI_1 = 0x90001018
    IO_REPARSE_TAG_GLOBAL_REPARSE = 0xA0000019
    IO_REPARSE_TAG_CLOUD = 0x9000001A
    IO_REPARSE_TAG_CLOUD_1 = 0x9000101A
    IO_REPARSE_TAG_CLOUD_2 = 0x9000201A
    IO_REPARSE_TAG_CLOUD_3 = 0x9000301A
    IO_REPARSE_TAG_CLOUD_4 = 0x9000401A
    IO_REPARSE_TAG_CLOUD_5 = 0x9000501A
    IO_REPARSE_TAG_CLOUD_6 = 0x9000601A
    IO_REPARSE_TAG_CLOUD_7 = 0x9000701A
    IO_REPARSE_TAG_CLOUD_8 = 0x9000801A
    IO_REPARSE_TAG_CLOUD_9 = 0x9000901A
    IO_REPARSE_TAG_CLOUD_A = 0x9000A01A
    IO_REPARSE_TAG_CLOUD_B = 0x9000B01A
    IO_REPARSE_TAG_CLOUD_C = 0x9000C01A
    IO_REPARSE_TAG_CLOUD_D = 0x9000D01A
    IO_REPARSE_TAG_CLOUD_E = 0x9000E01A
    IO_REPARSE_TAG_CLOUD_F = 0x9000F01A
    IO_REPARSE_TAG_APPEXECLINK = 0x8000001B
    IO_REPARSE_TAG_PROJFS = 0x9000001C
    IO_REPARSE_TAG_LX_SYMLINK = 0xA000001D
    IO_REPARSE_TAG_STORAGE_SYNC = 0x8000001E
    IO_REPARSE_TAG_WCI_TOMBSTONE = 0xA000001F
    IO_REPARSE_TAG_UNHANDLED = 0x80000020
    IO_REPARSE_TAG_ONEDRIVE = 0x80000021
    IO_REPARSE_TAG_PROJFS_TOMBSTONE = 0xA0000022
    IO_REPARSE_TAG_AF_UNIX = 0x80000023
    IO_REPARSE_TAG_LX_FIFO = 0x80000024
    IO_REPARSE_TAG_LX_CHR = 0x80000025
    IO_REPARSE_TAG_LX_BLK = 0x80000026
    IO_REPARSE_TAG_WCI_LINK = 0xA0000027
    IO_REPARSE_TAG_WCI_LINK_1 = 0xA0001027


class SymbolicLinkReparseBufferType(ctypes.Structure):
    _fields_ = [
        ("SubstituteNameOffset", wintypes.USHORT),
        ("SubstituteNameLength", wintypes.USHORT),
        ("PrintNameOffset", wintypes.USHORT),
        ("PrintNameLength", wintypes.USHORT),
        ("Flags", wintypes.DWORD),
        ("PathBuffer",
         ctypes.c_byte * (MAXIMUM_REPARSE_DATA_BUFFER_SIZE // 2 - 12))
    ]

    @property
    def substitute_name(self):
        return bytes(self.PathBuffer[self.SubstituteNameOffset:
                                     self.SubstituteNameOffset +
                                     self.SubstituteNameLength]) \
            .decode("utf-16")

    @property
    def print_name(self):
        return bytes(self.PathBuffer[self.PrintNameOffset:
                                     self.PrintNameOffset +
                                     self.PrintNameLength]) \
            .decode("utf-16")

    @property
    def is_relative(self):
        return self.Flags & SYMLINK_FLAG_RELATIVE != 0


class MountPointReparseBufferType(ctypes.Structure):
    _fields_ = [
        ("SubstituteNameOffset", wintypes.USHORT),
        ("SubstituteNameLength", wintypes.USHORT),
        ("PrintNameOffset", wintypes.USHORT),
        ("PrintNameLength", wintypes.USHORT),
        ("PathBuffer",
         ctypes.c_byte * (MAXIMUM_REPARSE_DATA_BUFFER_SIZE // 2 - 8))
    ]

    @property
    def substitute_name(self):
        return bytes(self.PathBuffer[self.SubstituteNameOffset:
                                     self.SubstituteNameOffset +
                                     self.SubstituteNameLength]) \
            .decode("utf-16")

    @property
    def print_name(self):
        return bytes(self.PathBuffer[self.PrintNameOffset:
                                     self.PrintNameOffset +
                                     self.PrintNameLength]) \
            .decode("utf-16")


class GenericReparseBufferType(ctypes.Structure):
    _fields_ = [
        ("DataBuffer", ctypes.c_byte * MAXIMUM_REPARSE_DATA_BUFFER_SIZE)
    ]


class LxSymlinkReparseBufferType(ctypes.Structure):
    _fields_ = [
        ("SubstituteNameOffset", wintypes.DWORD),
        ("PathBuffer",
         ctypes.c_byte * (MAXIMUM_REPARSE_DATA_BUFFER_SIZE - 4))
    ]

    def __init__(self, *args, **kw):
        super().__init__(args, kw)
        self.SubstituteNameLength = None

    def set_substitute_name_length(self, value):
        self.SubstituteNameLength = value

    @property
    def substitute_name(self):
        path_buffer = bytes(self.PathBuffer)
        if not hasattr(self, "SubstituteNameLength") or \
            self.SubstituteNameLength is None:
            # treat as a null-terminated string
            return path_buffer[:path_buffer.find(b"\x00")].decode("utf-8")
        else:
            return path_buffer[:self.SubstituteNameLength].decode("utf-8")


class ReparseBufferType(ctypes.Union):
    _fields_ = [
        ("SymbolicLinkReparseBuffer", SymbolicLinkReparseBufferType),
        ("MountPointReparseBuffer", MountPointReparseBufferType),
        ("GenericReparseBuffer", GenericReparseBufferType),
        ("LxSymlinkReparseBuffer", LxSymlinkReparseBufferType)
    ]


class REPARSE_DATA_BUFFER(ctypes.Structure):
    _fields_ = [
        ("ReparseTag", wintypes.DWORD),
        ("ReparseDataLength", wintypes.USHORT),
        ("Reserved", wintypes.USHORT),
        ("ReparseBuffer", ReparseBufferType)
    ]


def is_reparse_point(file_path: Union[str, bytes, os.PathLike]) -> bool:
    """
    Check if the given file path is a reparse point.

    :param file_path: The file path to check.
    :return: True if the file path is a reparse point, otherwise False.
    """
    file_path = os.fspath(file_path)
    try:
        h_file = CreateFile(
            file_path,
            GENERIC_READ,
            FILE_SHARE_READ,
            NULL,
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
            NULL
        )
    except OSError:
        return False
    if h_file == INVALID_HANDLE_VALUE:
        raise ctypes.WinError()

    try:
        data_buffer = REPARSE_DATA_BUFFER()
        bytes_returned = wintypes.DWORD()
        result = windll.kernel32.DeviceIoControl(
            h_file,
            FSCTL_GET_REPARSE_POINT,
            None,
            0,
            ctypes.byref(data_buffer),
            ctypes.sizeof(data_buffer),
            ctypes.byref(bytes_returned),
            None
        )
        return bool(result)
    finally:
        CloseHandle(h_file)


def get_reparse_info(file_path: Union[os.PathLike, str, bytes]) -> \
    Tuple[ReparseTag, Union[
        SymbolicLinkReparseBufferType,
        MountPointReparseBufferType,
        GenericReparseBufferType,
        LxSymlinkReparseBufferType]]:
    """
    Get reparse information for the given file path.

    :param file_path: The file path to retrieve reparse information for.
    :return: A tuple containing the reparse tag and a reparse buffer object.
    """
    file_path = os.fspath(file_path)
    h_file = CreateFile(
        file_path,
        GENERIC_READ,
        FILE_SHARE_READ,
        NULL,
        OPEN_EXISTING,
        FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
        NULL
    )
    if h_file == INVALID_HANDLE_VALUE:
        raise ctypes.WinError()

    try:
        data_buffer = REPARSE_DATA_BUFFER()
        bytes_returned = wintypes.DWORD()
        result = windll.kernel32.DeviceIoControl(
            h_file,
            FSCTL_GET_REPARSE_POINT,
            None,
            0,
            ctypes.byref(data_buffer),
            ctypes.sizeof(data_buffer),
            ctypes.byref(bytes_returned),
            None
        )
        if not result:
            raise ctypes.WinError()

        tag = ReparseTag(data_buffer.ReparseTag)
        if tag == ReparseTag.IO_REPARSE_TAG_SYMLINK:
            return tag, data_buffer.ReparseBuffer.SymbolicLinkReparseBuffer
        elif tag == ReparseTag.IO_REPARSE_TAG_MOUNT_POINT:
            return tag, data_buffer.ReparseBuffer.MountPointReparseBuffer
        elif tag == ReparseTag.IO_REPARSE_TAG_LX_SYMLINK:
            data_buffer.ReparseBuffer.LxSymlinkReparseBuffer \
                .set_substitute_name_length(data_buffer.ReparseDataLength - 4)
            return tag, data_buffer.ReparseBuffer.LxSymlinkReparseBuffer
        else:
            return tag, data_buffer.ReparseBuffer.GenericReparseBuffer
    finally:
        CloseHandle(h_file)
