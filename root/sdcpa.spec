# -*- mode: python ; coding: utf-8 -*-

import os
import sys
from PyInstaller.utils.hooks import collect_data_files, collect_submodules

block_cipher = None

# Add required imports
additional_imports = [
    'html5lib',
    'html5lib.treebuilders',
    'html5lib.treebuilders.etree',
    'html5lib.treewalkers',
    'html5lib.serializer',
    'html5lib.filters.sanitizer',
    'lxml',
    'lxml.etree',
    'lxml.html',
    'lxml.builder',
    'lxml._elementpath',
    'pyarrow',
    'pyarrow.lib',
    'pyarrow.pandas_compat'
]

# Make sure lxml data files are collected
datas_list = [
    ('src/auth/*.py', 'src/auth'),
    ('src/config/*.py', 'src/config'),
    ('src/config/res/amzn_req-main', 'src/config/res/amzn_req-main'),
    ('src/data/*.py', 'src/data'),
    ('src/ui/*.py', 'src/ui'),
    ('src/utils/*.py', 'src/utils'),
    ('src/logs', 'src/logs'),
    *collect_data_files('amzn_req'),
    *collect_data_files('html5lib'),
    *collect_data_files('lxml'),
    *collect_data_files('pyarrow')
]

# Add the src directory and amzn_req to Python path
src_path = os.path.abspath('./src')
amzn_req_path = os.path.abspath('./src/config/res/amzn_req-main')
sys.path.extend([src_path, amzn_req_path])

# Read requirements.txt with error handling
try:
    with open('requirements.txt', 'r', encoding='utf-8-sig') as f:
        requirements = []
        for line in f:
            try:
                if not line.startswith('AmznReq'):
                    requirements.append(line.strip())
            except UnicodeDecodeError:
                continue
except UnicodeDecodeError:
    # Fallback to different encoding if utf-8 fails
    try:
        with open('requirements.txt', 'r', encoding='latin-1') as f:
            requirements = [req.strip() for req in f.readlines() 
                          if not req.startswith('AmznReq')]
    except Exception as e:
        print(f"Error reading requirements.txt: {e}")
        requirements = []

def get_dependencies(packages):
    deps = set()
    for package in packages:
        if '@' in package:
            package = package.split('@')[0]
        package = package.replace('-', '_')
        deps.add(package)
    return list(deps)

# Explicitly define required packages if requirements parsing fails
base_requirements = [
    'PySide6',
    'numpy',
    'pandas',
    'requests',
    'selenium',
    'beautifulsoup4',
    'pillow',
    'polars'
]

all_dependencies = get_dependencies(requirements or base_requirements)

hiddenimports = [
    mod for mod in collect_submodules('PySide6')
    if any(needed in mod for needed in ['QtCore', 'QtGui', 'QtWidgets'])
]
hiddenimports.extend(all_dependencies + ['amzn_req'] + additional_imports)

a = Analysis(
    ['src/main.py'],  # Main entry point
    pathex=[
        '.',
        src_path,
        amzn_req_path
    ],
    binaries=[],
    datas=[
        ('src/auth/*.py', 'src/auth'),
        ('src/config/*.py', 'src/config'),
        ('src/config/res/amzn_req-main', 'src/config/res/amzn_req-main'),
        ('src/data/*.py', 'src/data'),
        ('src/ui/*.py', 'src/ui'),
        ('src/utils/*.py', 'src/utils'),
        ('src/logs', 'src/logs'),
        *collect_data_files('amzn_req')
    ],
    hiddenimports=hiddenimports,
    hookspath=['.'],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['FixTk', 'tcl', 'tk', '_tkinter', 'tkinter', 'Tkinter', 
          'matplotlib', 'notebook', 'scipy', 'numpy.random.tests',
          'PyQt5', 'PyQt4', 'IPython', 'jupyter', 'qt5', 'webkit'],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

# Exclude unnecessary files
a.binaries = [x for x in a.binaries if not any(
    pattern in x[0].lower() for pattern in [
        'opengl32sw.dll',
        'qt5web',
        'qt5quick',
        'qt5qml',
        'libglib',
        'libharfbuzz',
        'libicudata',
    ]
)]

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='SDCPickAssist',
    debug=False,
    bootloader_ignore_signals=False,
    strip=True,
    upx=True,
    upx_exclude=['vcruntime140.dll', 'python*.dll', 'api-ms-win-*.dll', 'ucrtbase.dll'],
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='src/config/res/ballac.ico'
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    Tree('src/auth', prefix='src/auth'),
    Tree('src/config', prefix='src/config'),
    Tree('src/data', prefix='src/data'),
    Tree('src/ui', prefix='src/ui'),
    Tree('src/utils', prefix='src/utils'),
    Tree('src/logs', prefix='src/logs'),
    strip=True,
    upx=True,
    upx_exclude=['vcruntime140.dll', 'python*.dll', 'api-ms-win-*.dll', 'ucrtbase.dll'],
    name='SDCPickAssist',
)
