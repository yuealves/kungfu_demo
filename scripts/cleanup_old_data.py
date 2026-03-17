#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
清理旧数据：扫描目录中的日期，只保留最近 N 个日期的数据，删除更早的。
不依赖任何第三方库，兼容 Python 2.7 / 3.x。

用法:
    python cleanup_old_data.py [--keep N] [--dry-run]

默认保留最近 3 个日期。--dry-run 只打印不删除。
"""

from __future__ import print_function
import os
import re
import sys
import shutil

# ── 配置 ──────────────────────────────────────────────────────
# (路径, 模式)
#   "subdir"  — 日期是子目录名 (20260316/)
#   "prefix"  — 日期是文件名前缀 (20260316_xxx.parquet)
TARGETS = [
    ("/data/shenrun/dump_1m_parquet", "subdir"),
    ("/data/shenrun/dump_parquet",    "prefix"),
]

DEFAULT_KEEP = 3
DATE_RE = re.compile(r"^(\d{8})")
# ──────────────────────────────────────────────────────────────


def collect_dates_subdir(base_dir):
    """收集子目录名为 YYYYMMDD 的日期集合"""
    dates = set()
    for name in os.listdir(base_dir):
        if DATE_RE.match(name) and os.path.isdir(os.path.join(base_dir, name)):
            dates.add(name[:8])
    return dates


def collect_dates_prefix(base_dir):
    """收集文件名以 YYYYMMDD 开头的日期集合"""
    dates = set()
    for name in os.listdir(base_dir):
        m = DATE_RE.match(name)
        if m and os.path.isfile(os.path.join(base_dir, name)):
            dates.add(m.group(1))
    return dates


def cleanup(base_dir, mode, keep, dry_run):
    if not os.path.isdir(base_dir):
        print("[skip] %s (not found)" % base_dir)
        return

    if mode == "subdir":
        dates = collect_dates_subdir(base_dir)
    else:
        dates = collect_dates_prefix(base_dir)

    if not dates:
        print("[skip] %s (no date entries)" % base_dir)
        return

    sorted_dates = sorted(dates, reverse=True)
    keep_dates = set(sorted_dates[:keep])
    remove_dates = sorted(set(sorted_dates) - keep_dates)

    print("[%s] found %d dates, keep %s, remove %s" % (
        base_dir, len(sorted_dates),
        sorted(keep_dates), remove_dates or "(none)"))

    if not remove_dates:
        return

    removed_count = 0

    if mode == "subdir":
        for d in remove_dates:
            target = os.path.join(base_dir, d)
            if os.path.isdir(target):
                if dry_run:
                    print("  [dry-run] would remove dir %s" % target)
                else:
                    shutil.rmtree(target)
                    print("  removed dir %s" % target)
                removed_count += 1
    else:
        for name in sorted(os.listdir(base_dir)):
            m = DATE_RE.match(name)
            if m and m.group(1) in remove_dates:
                target = os.path.join(base_dir, name)
                if os.path.isfile(target):
                    if dry_run:
                        print("  [dry-run] would remove %s" % name)
                    else:
                        os.remove(target)
                    removed_count += 1

    print("  %s %d items" % ("would remove" if dry_run else "removed", removed_count))


def main():
    keep = DEFAULT_KEEP
    dry_run = False

    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "--keep" and i + 1 < len(args):
            keep = int(args[i + 1])
            i += 2
        elif args[i] == "--dry-run":
            dry_run = True
            i += 1
        elif args[i] in ("-h", "--help"):
            print("Usage: %s [--keep N] [--dry-run]" % sys.argv[0])
            return
        else:
            print("Unknown arg: %s" % args[i], file=sys.stderr)
            sys.exit(1)

    if dry_run:
        print("=== DRY RUN (no files will be deleted) ===")

    for base_dir, mode in TARGETS:
        cleanup(base_dir, mode, keep, dry_run)


if __name__ == "__main__":
    main()
