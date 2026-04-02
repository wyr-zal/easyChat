import argparse
import subprocess
import sys
from pathlib import Path


def build_command(args: argparse.Namespace) -> list[str]:
    project_root = Path(__file__).resolve().parent
    entry_script = project_root / "excel_sender_gui.py"

    command = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--noconfirm",
        "--clean",
        "--name",
        args.name,
    ]

    if args.onefile:
        command.append("--onefile")
    else:
        command.append("--onedir")

    if not args.console:
        command.append("--windowed")

    if args.icon:
        icon_path = Path(args.icon).expanduser().resolve()
        command.extend(["--icon", str(icon_path)])

    command.append(str(entry_script))
    return command


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="打包 Excel 个性化群发窗口为可分发的 Windows 可执行文件。"
    )
    parser.add_argument(
        "--name",
        default="EasyChatExcelSender",
        help="输出程序名称，默认：EasyChatExcelSender",
    )
    parser.add_argument(
        "--onefile",
        action="store_true",
        default=True,
        help="打包为单文件 exe，默认开启。",
    )
    parser.add_argument(
        "--onedir",
        dest="onefile",
        action="store_false",
        help="改为目录模式输出。",
    )
    parser.add_argument(
        "--console",
        action="store_true",
        help="保留控制台窗口，默认关闭。",
    )
    parser.add_argument(
        "--icon",
        help="可选：指定 ico 图标路径。",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    command = build_command(args)

    print("开始执行打包命令：")
    print(" ".join(f'"{part}"' if " " in part else part for part in command))

    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
