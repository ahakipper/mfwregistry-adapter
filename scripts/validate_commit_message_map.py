#!/usr/bin/env python3
"""Validate the reviewed commit-message map against a before snapshot."""
import re
import sys
from collections import Counter
from pathlib import Path

HEADINGS = ["Problem:", "Changes:", "Verification:", "Compatibility / Rollback:", "Documentation:"]
HASH_RE = re.compile(r"^### ([0-9a-f]{40})\s*$", re.M)
CJK_RE = re.compile(r"[\u3400-\u9fff]")

def main():
    if len(sys.argv) != 3:
        print("usage: validate_commit_message_map.py BEFORE.tsv MAP.md", file=sys.stderr)
        return 2
    before = Path(sys.argv[1]).read_text()
    mapping = Path(sys.argv[2]).read_text()
    rows = [line.split("\t", 2) for line in before.splitlines() if line.strip()]
    expected = [row[0] for row in rows]
    bodies = {row[0]: (row[2] if len(row) > 2 else "") for row in rows}
    found = HASH_RE.findall(mapping)
    if found != expected:
        print(f"commit order/content mismatch: expected {len(expected)}, found {len(found)}", file=sys.stderr)
        return 1
    sections = re.split(r"^### [0-9a-f]{40}\s*$", mapping, flags=re.M)[1:]
    problems = []
    for commit, section in zip(found, sections):
        if "The change addresses the repository change described by its original diff and subject:" in section:
            print(f"boilerplate problem: {commit}", file=sys.stderr); return 1
        subject = next((line.strip() for line in section.splitlines() if line.strip()), "")
        if CJK_RE.search(subject):
            print(f"CJK subject: {commit}", file=sys.stderr); return 1
        if ":" not in subject or len(subject) < 18 or re.match(r"^(fix|update|changes|test|docs|chore)\s*:\s*(fix|update|changes|misc)?\s*$", subject, re.I):
            print(f"generic subject: {commit}: {subject}", file=sys.stderr); return 1
        for heading in HEADINGS:
            if not re.search(r"^" + re.escape(heading) + r"$", section, re.M):
                print(f"missing {heading} for {commit}", file=sys.stderr); return 1
        pm = re.search(r"^Problem:\n(.*)$", section, re.M)
        problem = pm.group(1).strip() if pm else ""
        # Reject the generic templates used by earlier map revisions.  The
        # check intentionally examines the leading clause after normalizing
        # subject/path suffixes, so appending a commit-specific phrase cannot
        # make a boilerplate Problem line pass review.
        normalized = re.sub(r"\s+", " ", problem).strip().lower()
        generic_prefixes = (
            "before this change",
            "before this commit",
            "left a concrete pre-change risk",
            "the intended behavior could",
            "the change addresses the repository change described",
            "the pre-commit state lacked this concrete behavior",
            "the previous behavior still allowed or lacked",
        )
        generic_fragments = (
            "left a concrete pre-change risk",
            "the intended behavior could be missing",
            "the change addresses the repository change described",
        )
        if normalized.startswith(generic_prefixes) or any(fragment in normalized for fragment in generic_fragments):
            print(f"generic Problem prefix: {commit}", file=sys.stderr)
            return 1
        problems.append(problem)
        verification = re.search(r"^Verification:\n(.*?)(?=\n\n[A-Z].*?:$|\Z)", section, re.M | re.S)
        text = verification.group(1).lower() if verification else ""
        if re.search(r"\b(pass|passed|success|exit 0|go test|go vet)\b", text):
            if not re.search(r"\b(pass|passed|success|exit 0|go test|go vet)\b", bodies.get(commit, "").lower()):
                print(f"unsubstantiated verification claim: {commit}", file=sys.stderr); return 1
    counts = Counter(problems)
    if any(n > 3 for n in counts.values()):
        print("repeated generic Problem lines", file=sys.stderr); return 1
    for forbidden in ("Nacos integration lacked", "Provider handling could"):
        if any(forbidden.lower() in p.lower() for p in problems):
            print(f"generic Problem phrase: {forbidden}", file=sys.stderr); return 1
    print(f"validated {len(found)} commit-message entries")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
