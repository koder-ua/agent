#!/usr/bin/env python3
import sys
import stat
import shutil
import hashlib
import tempfile
import argparse
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Any, Set, Dict
from distutils.spawn import find_executable


EXT_TO_REMOVE = {".c", ".pyi", ".h", '.cpp', '.hpp', '.dist-info', '.pyx', '.pxd'}
modules_to_remove = ['ensurepip', 'lib2to3', 'venv', 'tkinter']

@dataclass
class ArchConfig:
    sources: List[str]
    remove_files: List[str] = field(default_factory=lambda: EXT_TO_REMOVE.copy())
    requirements: str = ""
    version: str = f"{sys.version_info.major}.{sys.version_info.minor}"
    unpack: str = ""
    remove_modules: List[str] = field(default_factory=lambda: modules_to_remove[:])
    remove_pycache: bool = True
    remove_config: bool = True


def load_config(path: Path) -> ArchConfig:
    attrs: Dict[str, Any] = {}
    curr_line = ""

    for idx, line in enumerate(path.open()):
        if '#' in line:
            line = line.split("#", 1)[0]

        line = line.strip()

        if line == '':
            continue

        if line.endswith('\\'):
            curr_line += line[:-1] + " "
            continue
        else:
            curr_line += line

        if '=' not in curr_line:
            raise ValueError(f"Syntax error in line {idx} in file {path}. '=' expected in line {curr_line}")

        curr_line = curr_line.strip()

        var, val = curr_line.split("=", 1)
        var = var.strip()
        if var in {'sources', 'remove_files', 'remove_modules'}:
            attrs[var.strip()] = val.split()
        elif var in {'requirements', 'unpack', 'version'}:
            attrs[var.strip()] = val.strip()
        elif var in {'remove_pycache', 'remove_config'}:
            val = val.strip()
            if val not in ('True', 'False'):
                raise ValueError(f"Syntax error at line {idx} in file {path}. " +
                                 f"Value of {var} can only be 'True' or 'False', not {val}")
            attrs[var] = val == 'True'
        else:
            raise ValueError(f"Syntax error in line {idx} in file {path}. Unknown key {var!r}")

        curr_line = ""

    if curr_line != '':
            raise ValueError(f"Syntax error in file {path}. Extra data at the end ('\\' in the last line?)")

    return ArchConfig(**attrs)


def install_deps(target: Path, py_name: str, requirements: Path, libs_dir_name: str):
    tempo_libs = target / 'tmp_libs'
    tempo_libs.mkdir(parents=True, exist_ok=True)
    cmd = f"{py_name} -m pip install --no-compile --ignore-installed --prefix {tempo_libs} -r {requirements}"
    subprocess.check_call(cmd, shell=True)

    for fname in list(tempo_libs.rglob("*")):
        if fname.suffix in EXT_TO_REMOVE:
            if fname.exists():
                if fname.is_dir():
                    shutil.rmtree(fname, ignore_errors=True)
                else:
                    fname.unlink()

    libs_target = target / libs_dir_name
    site_packages = tempo_libs / 'lib' / py_name / 'site-packages'
    if not site_packages.exists():
        print("No libs installed by pip. If project has no requirements - remove requirements = XXX line from config")
        exit(1)

    site_packages.rename(libs_target)
    shutil.rmtree(tempo_libs, ignore_errors=True)


def copy_files(root_dir: Path, target: Path, patterns: List[str]):
    copy_anything = False
    for pattern in patterns:
        names = list(root_dir.glob(pattern))
        for name in names:
            copy_anything = True
            target_fl = target / name.relative_to(root_dir)
            target_fl.parent.mkdir(parents=True, exist_ok=True)
            if name.is_file():
                shutil.copyfile(name, target_fl)
            elif name.is_dir():
                shutil.copytree(name, target_fl, symlinks=False)

        if '*' not in pattern and '?' not in pattern and names == []:
            print(f"Failed: can't find name {pattern}")
            exit(1)

        print(f"Adding {root_dir}/{pattern} => {len(names)} files/dirs")

    if not copy_anything:
        print("Failed: Find nothing to copy to archive")
        exit(1)


def copy_py_binary(py_name: str, bin_target: Path):
    py_path = find_executable(py_name)
    assert py_path is not None
    shutil.copyfile(py_path, bin_target)
    bin_target.chmod(stat.S_IXUSR | stat.S_IRUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)


def copy_py_lib(py_name: str, lib_target: Path):
    cmd = [py_name, "-c", "import sys; print('\\n'.join(sys.path))"]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    assert result.returncode == 0, result.stdout

    for line in result.stdout.decode("utf8").split("\n"):
        line = line.strip()
        if not line:
            continue
        lib_path = Path(line)
        if (lib_path / 'encodings').is_dir():
            break
    else:
        raise RuntimeError(f"Can't find std library path for {py_name}")

    shutil.copytree(src=str(lib_path), dst=str(lib_target))


def clear_lib(lib_target: Path, cfg: ArchConfig):
    for name in cfg.remove_modules:
        tgt = lib_target / name
        if tgt.is_dir():
            shutil.rmtree(tgt)

    if cfg.remove_config:
        for itemname in lib_target.iterdir():
            if itemname.name.startswith("config-") and itemname.is_dir():
                shutil.rmtree(itemname)

    if cfg.remove_pycache:
        for pycache_name in lib_target.rglob("__pycache__"):
            if pycache_name.is_dir():
                shutil.rmtree(pycache_name)


def parse_arge(argv: List[str]) -> Any:
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--standalone", action="store_true",
                        help="Make standalone archive with python interpreter and std library")
    parser.add_argument("--config", default=None, help="Archive config file")
    parser.add_argument("base_folder", help="Path to package directory (it parent must contains requirements.txt)")
    parser.add_argument("arch_path", help="Path to store self-unpacking archive to")
    return parser.parse_args(argv[1:])


def main(argv: List[str]) -> int:
    opts = parse_arge(argv)
    root_dir = Path(opts.base_folder).absolute()
    config = opts.config if opts.config else root_dir / 'arch_config.txt'
    cfg = load_config(Path(config))
    py_name = f"python{cfg.version}"

    requirements = root_dir / cfg.requirements if cfg.requirements else None
    self_unpack_sh = root_dir / cfg.unpack if cfg.unpack else Path(__file__).absolute().parent.parent / 'unpack.sh'
    libs_dir_name = "libs"

    if requirements and not requirements.exists():
        print(f"Can't find requirements at {requirements}")
        return 1

    if not self_unpack_sh.exists():
        print(f"Can't find unpack shell file at {self_unpack_sh}")
        return 1

    target = Path(tempfile.mkdtemp())
    arch_target = Path(opts.arch_path).absolute()
    temp_arch_target = arch_target.parent / (arch_target.name + ".tmp")

    copy_files(root_dir, target, cfg.sources)

    if requirements:
        install_deps(target, py_name, requirements, libs_dir_name)

    if opts.standalone:
        standalone_root = target / 'python'
        standalone_root.mkdir(parents=True, exist_ok=True)
        standalone_stdlib = target / 'python' / 'lib' / py_name
        standalone_stdlib.parent.mkdir(parents=True, exist_ok=True)
        copy_py_binary(py_name, standalone_root / py_name)
        copy_py_lib(py_name, standalone_stdlib)
        clear_lib(standalone_stdlib, cfg)

    if cfg.remove_pycache:
        for pycache_name in target.rglob("__pycache__"):
            if pycache_name.is_dir():
                shutil.rmtree(pycache_name)

    tar_cmd = ["tar", "--create", "--gzip", "--directory=" + str(target), "--file", str(temp_arch_target),
               *(item.name for item in target.iterdir())]

    if opts.standalone:
        tar_cmd.append("python")

    subprocess.run(tar_cmd).check_returncode()

    with arch_target.open("wb") as target_fd:
        with temp_arch_target.open("rb") as source_arch:
            with self_unpack_sh.open("rb") as source_sh:
                shutil.copyfileobj(source_sh, target_fd, length=2 ** 20)
                shutil.copyfileobj(source_arch, target_fd, length=2 ** 20)

    hashobj = hashlib.md5()
    with arch_target.open("rb") as target_fd:
        while True:
            data = target_fd.read(2 ** 20)
            if not data:
                break
            hashobj.update(data)

    temp_arch_target.unlink()
    shutil.rmtree(str(target))

    print(f"Results stored into {arch_target}. Size = {arch_target.stat().st_size} bytes. MD5 {hashobj.hexdigest()}")

    return 0


if __name__ == "__main__":
    exit(main(sys.argv))
