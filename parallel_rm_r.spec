# -*- mode: python ; coding: utf-8 -*-


block_cipher = None


a = Analysis(
    ['parallel_rm_r.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['concurrent.futures.process', 'subprocess', 'email', 'pickle', 'ssl', 'socket', 'shlex', 'http', 'gzip', 'bz2', 'lzma', 'netrc', 'tarfile', 'zipfile', 'ftplib', 'csv', 'argparse', 'hashlib', 'fractions', 'dis', 'decimal', 'calendar', 'ast', 'struct', '_py_abc', 'base64', 'bisect', 'getopt', 'getpass', 'gettext', 'stringprep', 'mimetypes', 'random', 'statistics', 'tempfile', 'tracemalloc', 'datetime', '_pydecimal', '_strptime', 'copy', 'contextvars', 'nturl2path', 'numbers', 'opcode', 'urllib.request', 'urllib.response', 'quopri', 'encodings.quopri_codec', 'encodings.idna', 'encodings.bz2_codec', 'encodings.base64_codec', 'abc', 'posix', 'posixpath'],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='parallel_rm_r',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='NONE',
)
