# -*- coding: utf-8 -*-
"""
诊断脚本：扫描微信窗口，定位 "+ (快捷操作)" 按钮的真实位置。

适用场景：
- 群聊管理报错 "未找到 '+' (快捷操作) 按钮"
- 怀疑微信版本/UI 变了，硬编码的 Depth=11 不再适用
- 想知道当前 UI 树里"+"按钮在哪一层、Name 是什么

用法：
    1. 先打开并登录微信，让主窗口可见。
    2. （强烈建议）开启 Windows 讲述人 Win+Ctrl+Enter
    3. 在终端执行：
           python diagnose_plus_btn.py
       默认 zh-CN；如果你的微信是英文界面：
           python diagnose_plus_btn.py --lang en-US
    4. 看输出报告 —— 会列出所有按钮、并把命中关键词的按钮标红。
"""

import argparse
import sys
import time
from ctypes import windll

import uiautomation as auto

# Windows 控制台默认 GBK，强制 stdout/stderr 为 UTF-8，避免 emoji/特殊字符崩溃
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

try:
    from wechat_locale import WeChatLocale
except Exception as e:
    print("[ERROR] 找不到 wechat_locale 模块，请把脚本放到项目根目录运行。", e)
    sys.exit(1)


# 命中即视为"+按钮"候选的关键词
HOT_NAMES = ("快捷操作", "Quick Action", "快捷操作 (Alt+A)", "+", "更多", "More")


def banner(title):
    print("\n" + "=" * 8 + f" {title} " + "=" * 8)


def is_wechat_visible(win) -> dict:
    info = {"exists": False, "visible": False, "minimized": False, "hwnd": 0}
    if not win.Exists(0, 0):
        return info
    info["exists"] = True
    hwnd = win.NativeWindowHandle
    info["hwnd"] = hwnd
    u32 = windll.user32
    info["visible"] = bool(u32.IsWindowVisible(hwnd))
    info["minimized"] = bool(u32.IsIconic(hwnd))
    return info


def force_show(win):
    try:
        u32 = windll.user32
        hwnd = win.NativeWindowHandle
        if u32.IsIconic(hwnd):
            u32.ShowWindow(hwnd, 9)  # SW_RESTORE
        u32.SetForegroundWindow(hwnd)
        win.SetFocus()
        time.sleep(0.4)
    except Exception as e:
        print(f"[WARN] 强制显示窗口失败：{e}")


def walk(control, depth, max_depth, results):
    """递归遍历控件树，把所有按钮记录下来。"""
    try:
        ctype = control.ControlTypeName or ""
        if "Button" in ctype:
            results.append({
                "depth": depth,
                "name": control.Name or "",
                "class_name": control.ClassName or "",
                "automation_id": control.AutomationId or "",
                "ctype": ctype,
                "rect": control.BoundingRectangle,
            })
    except Exception:
        pass

    if depth >= max_depth:
        return
    try:
        for child in control.GetChildren():
            walk(child, depth + 1, max_depth, results)
    except Exception:
        pass


def main():
    parser = argparse.ArgumentParser(description="诊断微信 '+' 按钮位置")
    parser.add_argument("--lang", default="zh-CN", choices=["zh-CN", "zh-TW", "en-US"])
    parser.add_argument("--max-depth", type=int, default=20, help="最大递归深度，默认 20")
    args = parser.parse_args()

    lc = WeChatLocale(args.lang)
    expected_name = lc.quick_action

    banner("Step 1. 查找微信主窗口")
    win = auto.WindowControl(Depth=1, Name=lc.weixin, searchDepth=1)
    info = is_wechat_visible(win)
    print(f"窗口存在: {info['exists']}")
    print(f"窗口可见: {info['visible']}")
    print(f"窗口最小化: {info['minimized']}")
    print(f"HWND: {info['hwnd']}")
    if not info["exists"]:
        print("[FATAL] 找不到微信主窗口，请先打开并登录微信。")
        sys.exit(2)

    banner("Step 2. 强制激活窗口")
    force_show(win)
    print("已尝试 SW_RESTORE + SetForegroundWindow + SetFocus。")
    time.sleep(0.3)

    banner("Step 3. 扫描微信窗口下所有按钮")
    print(f"语言包: {args.lang}")
    print(f"代码期望的 Name = '{expected_name}'  (当前硬编码 Depth=11)")
    print("正在递归遍历...")
    results = []
    walk(win, 1, args.max_depth, results)
    print(f"共扫描到 {len(results)} 个按钮控件。")

    banner("Step 4. 高亮可疑命中（'+按钮' 候选）")
    hits = []
    for r in results:
        name = r["name"]
        if not name:
            continue
        if any(k in name for k in HOT_NAMES):
            hits.append(r)

    if not hits:
        print("[!] 没有任何按钮 Name 命中关键词。")
        print("    这说明 (a) 讲述人没开 → uiautomation 拿不到 Name；")
        print("           或 (b) 微信改名了/UI变了。")
    else:
        print(f"找到 {len(hits)} 个候选按钮：")
        print(f"{'Depth':>5}  {'Name':30}  {'ClassName':35}  {'AutomationId':25}  Rect")
        for r in hits:
            print(
                f"{r['depth']:>5}  {r['name'][:30]:30}  "
                f"{r['class_name'][:35]:35}  {r['automation_id'][:25]:25}  {r['rect']}"
            )

    banner("Step 5. 完整按钮清单（按 Depth 排序，Top 60）")
    results_sorted = sorted(results, key=lambda x: (x["depth"], x["name"]))
    print(f"{'Depth':>5}  {'Name':30}  {'ClassName':35}  {'AutomationId':25}")
    for r in results_sorted[:60]:
        print(
            f"{r['depth']:>5}  {(r['name'] or '<无 Name>')[:30]:30}  "
            f"{(r['class_name'] or '<无>'):35}  {(r['automation_id'] or '<无>'):25}"
        )
    if len(results_sorted) > 60:
        print(f"... 还有 {len(results_sorted) - 60} 个按钮未显示（深度更深）")

    banner("结论建议")
    if hits:
        print("[OK] 把日志中命中的那个按钮的 Name + Depth + ClassName + AutomationId 发给我。")
        print("     我会用它替换 group_manager_service.py 第 ~203 行的硬编码定位。")
    else:
        print("[NG] 没找到候选按钮。请先确认：")
        print("     1) Windows 讲述人是否启用？(Win+Ctrl+Enter)")
        print("     2) 微信是不是当前在前台？")
        print("     3) 把 Step 5 的按钮清单全部截图发我。")


if __name__ == "__main__":
    main()
