import unittest
from typing import Optional

from pygissim.util import *
from pygissim.engine import *

class TestLibraries(unittest.TestCase):
    def test_load(self):
        lib: LibManager = LibManager()
        lib.load_local()
        self.assertTrue(len(lib.hardware) > 0)
        self.assertTrue(len(lib.service_definitions) > 0)
        self.assertTrue(len(lib.workflow_steps) > 0)
        self.assertTrue(len(lib.workflow_chains) > 0)
        self.assertTrue(len(lib.workflow_definitions) > 0)
        