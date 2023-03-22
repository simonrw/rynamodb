#!/usr/bin/env python

"""
This script takes the report from the compliance tests and
renders it so that github actions can read it and post it.
"""


import argparse
import textwrap
import xml.sax
from xml.sax import ContentHandler


class Visitor(ContentHandler):
    def __init__(self):
        self.errors = 0
        self.failures = 0
        self.skipped = 0
        self.tests = 0
        self.time = 0.0

    def write_to(self, out_stream):
        tpl = textwrap.dedent(
            f"""
            # Compliance test report

            | Errors | {self.errors} |
            | Failed | {self.failures} |
            | Skipped | {self.skipped} |
            | Passed | {self.passed} |

            Time taken: {self.time:.2f} s

            **Pass rate: {self.pass_rate:.1f} %**
            """
        )
        out_stream.write(tpl)

    @property
    def passed(self) -> int:
        return self.tests - (self.errors + self.failures + self.skipped)

    @property
    def pass_rate(self) -> float:
        return self.passed * 100.0 / self.tests

    # visitor methods
    def startElement(self, name, attrs):
        if name != "testsuite":
            return

        self.errors += int(attrs.get("errors", 0))
        self.failures += int(attrs.get("failures", 0))
        self.skipped += int(attrs.get("skipped", 0))
        self.tests += int(attrs.get("tests", 0))
        self.time += float(attrs.get("time", 0.0))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", type=argparse.FileType("r"))
    parser.add_argument("-o", "--output", type=argparse.FileType("w"), default="-")
    args = parser.parse_args()

    visitor = Visitor()
    xml.sax.parse(args.filename, visitor)
    visitor.write_to(args.output)
