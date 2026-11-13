"""Skip helpers: some scenarios use Linux kernel features on purpose (/proc, AF_UNIX sockets,
SIGKILL/SIGCHLD, flock, selectors on pipes). The handbook's platform is Ubuntu 24.04; on any other
OS those tests are reported as skipped with the reason instead of failing at random."""
import sys
import unittest

IS_LINUX = sys.platform.startswith("linux")


def linux_only(reason):
    return unittest.skipUnless(IS_LINUX, f"needs Linux: {reason}")
