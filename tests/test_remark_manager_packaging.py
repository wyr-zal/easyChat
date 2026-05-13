# -*- coding: utf-8 -*-

import unittest
from pathlib import Path


class RemarkManagerPackagingTest(unittest.TestCase):
    def test_pack_script_builds_remark_manager_from_project_root(self):
        project_root = Path(__file__).resolve().parents[1]
        script_path = project_root / "remark_manager" / "pack_remark_manager.bat"

        self.assertTrue(script_path.exists(), "缺少备注批量修改独立打包脚本")

        script = script_path.read_text(encoding="utf-8")
        self.assertIn('cd /d "%~dp0\\.."', script)
        self.assertIn("python -m PyInstaller", script)
        self.assertIn("--onefile", script)
        self.assertIn("--windowed", script)
        self.assertIn("--name remark_manager", script)
        self.assertIn(r"remark_manager\remark_manager_gui.py", script)
        self.assertIn(r"dist\remark_manager.exe", script)
        self.assertIn("pause", script.lower())


if __name__ == "__main__":
    unittest.main()
