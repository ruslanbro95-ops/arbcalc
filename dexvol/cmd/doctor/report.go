package main

import (
	"fmt"
	"os"
	"strings"
)

// level is how badly a failed check matters.
type level int

const (
	levelOK level = iota
	// levelWarn is something to look at but not a reason to refuse to start:
	// a link pattern that could not be confirmed, a probe that came back
	// empty. These are exactly the things this project could not verify from
	// its build environment.
	levelWarn
	// levelFail means the service cannot do its job until it is fixed.
	levelFail
)

func (l level) mark() string {
	switch l {
	case levelOK:
		return "ok  "
	case levelWarn:
		return "warn"
	default:
		return "FAIL"
	}
}

type check struct {
	group  string
	name   string
	level  level
	detail string
	// hint is what to actually do about it.
	hint string
}

// report collects results so the whole picture is printed at once, rather than
// making someone fix one thing, rerun, and discover the next.
type report struct {
	checks []check
}

func (r *report) add(group, name string, lv level, detail, hint string) {
	r.checks = append(r.checks, check{group: group, name: name, level: lv, detail: detail, hint: hint})
}

func (r *report) ok(group, name, detail string)         { r.add(group, name, levelOK, detail, "") }
func (r *report) warn(group, name, detail, hint string) { r.add(group, name, levelWarn, detail, hint) }
func (r *report) fail(group, name, detail, hint string) { r.add(group, name, levelFail, detail, hint) }

// print writes the table and returns the number of hard failures.
func (r *report) print() int {
	width := 0
	for _, c := range r.checks {
		if n := len(c.name); n > width {
			width = n
		}
	}

	var (
		fails, warns int
		lastGroup    string
	)
	for _, c := range r.checks {
		if c.group != lastGroup {
			fmt.Printf("\n%s\n", c.group)
			lastGroup = c.group
		}
		fmt.Printf("  %s  %-*s  %s\n", c.level.mark(), width, c.name, c.detail)
		switch c.level {
		case levelFail:
			fails++
		case levelWarn:
			warns++
		}
	}

	// One hint per distinct remedy, naming everything it covers. Repeating
	// "set RPC_X to a reachable endpoint" nine times buries the one line that
	// is actually different.
	var order []string
	covered := map[string][]string{}
	for _, c := range r.checks {
		if c.hint == "" {
			continue
		}
		if _, seen := covered[c.hint]; !seen {
			order = append(order, c.hint)
		}
		covered[c.hint] = append(covered[c.hint], c.name)
	}
	if len(order) > 0 {
		fmt.Println("\nWhat to do")
		for _, hint := range order {
			fmt.Printf("  %s\n    %s\n", strings.Join(covered[hint], ", "), hint)
		}
	}

	fmt.Printf("\n%d checks, %d failed, %d warnings\n", len(r.checks), fails, warns)
	if fails == 0 && warns == 0 {
		fmt.Println("Ready to run: ./monitor")
	} else if fails == 0 {
		fmt.Println("No blockers. The warnings above are worth a look but the service will run.")
	} else {
		fmt.Fprintln(os.Stderr, "Fix the failures above before starting the service.")
	}
	return fails
}
