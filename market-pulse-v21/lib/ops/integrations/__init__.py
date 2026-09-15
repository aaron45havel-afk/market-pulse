"""Vendor adapters. One module per vendor, each with a fake.

CLAUDE.md and ARCHITECTURE.md §3 both ask for this shape, and the reason
is the same every time: a test that hits a vendor sandbox is a test that
fails when the vendor has an outage, and a codebase whose only path to
sending an email is a live API key is a codebase where nobody runs the
tests.

Every adapter here follows the same rule. The FAKE is the default. The
real one is constructed only when its credentials are present in the
environment, and if they are absent the module says so rather than
falling back silently — a fake that quietly stands in for a real
notification is how a rent reminder stops going out for a month before
anybody notices.
"""
