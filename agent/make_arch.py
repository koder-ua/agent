#!/usr/bin/env python3
import sys
import stat
import shutil
import hashlib
import tempfile
import argparse
import subprocess
from pathlib import Path
from typing import List, Any, Set
from distutils.spawn import find_executable


EXT_TO_REMOVE = {".c", ".pyi", ".h", '.cpp', '.hpp', '.dist-info', '.pyx', '.pxd'}


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
    (tempo_libs / 'lib' / py_name / 'site-packages').rename(libs_target)
    shutil.rmtree(tempo_libs, ignore_errors=True)


def copy_code(package_dir: Path, target: Path, root_dir: Path):
    for name in package_dir.rglob("*.py"):
        target_fl = target / name.relative_to(root_dir)
        target_fl.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(name, target_fl)


def copy_py_binary(py_name: str, bin_target: Path):
    shutil.copyfile(find_executable(py_name), bin_target)
    bin_target.chmod(stat.S_IXUSR | stat.S_IRUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)


def copy_extra(root_dir: Path, target: Path) -> Set[str]:
    extra_path = root_dir / 'arch_extra.txt'
    extra = set()
    if extra_path.is_file():
        for line in extra_path.open():
            name = line.strip()
            if name and not name.startswith("#"):
                assert '/' not in name
                shutil.copyfile(str(root_dir / name), str(target / name))
                extra.add(name)
    return extra


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

    for name in ('ensurepip', 'lib2to3', 'venv', 'tkinter'):
        tgt = lib_target / name
        if tgt.is_dir():
            shutil.rmtree(tgt)

    for name in lib_target.iterdir():
        if name.name.startswith("config-") and name.is_dir():
            shutil.rmtree(name)

    for name in lib_target.rglob("__pycache__"):
        if name.is_dir():
            shutil.rmtree(name)


def parse_arge(argv: List[str]) -> Any:
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--python-version", default=f"{sys.version_info.major}.{sys.version_info.minor}",
                        help="Path to store archive to")
    parser.add_argument("--standalone", action="store_true",
                        help="Make standalone archive with python interpreter and std library")
    parser.add_argument("package_path", help="Path to package directory (it parent must contains requirements.txt)")
    parser.add_argument("arch_path", help="Path to store self-unpacking archive to")
    return parser.parse_args(argv[1:])


def main(argv: List[str]) -> int:
    opts = parse_arge(argv)
    py_name = f"python{opts.python_version}"
    package_dir = Path(opts.package_path).absolute()
    root_dir = package_dir.parent
    requirements = root_dir / 'requirements.txt'
    self_unpack_sh = root_dir / 'unpack.sh'
    libs_dir_name = "libs"

    if not requirements.exists():
        print(f"Can't find requirements at {requirements}")
        return 1

    target = Path(tempfile.mkdtemp())
    arch_target = Path(opts.arch_path).absolute()
    temp_arch_target = arch_target.parent / (arch_target.name + ".tmp")

    copy_code(package_dir, target, root_dir)
    install_deps(target, py_name, requirements, libs_dir_name)

    if opts.standalone:
        standalone_root = target / 'python'
        standalone_root.mkdir(parents=True, exist_ok=True)
        standalone_stdlib = target / 'python' / 'lib' / py_name
        standalone_stdlib.parent.mkdir(parents=True, exist_ok=True)

        copy_py_binary(py_name, standalone_root / py_name)
        copy_py_lib(py_name, standalone_stdlib)

    extra = copy_extra(root_dir, target)

    tar_cmd = ["tar", "--create", "--gzip", "--directory=" + str(target), "--file", str(temp_arch_target),
               libs_dir_name, package_dir.name] + list(extra)

    if opts.standalone:
        tar_cmd.append("python")

    assert subprocess.run(tar_cmd).returncode == 0

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
