// Command audit checks, per grid combination and without running the
// emulator, which sampled task templates fit no server and which of the
// replayed workflows can never complete (capacity or time). Optionally it
// joins the verdicts with existing run results to measure how much of the
// observed incompleteness they explain.
//
//	go run ./cmd/audit -configs <grid>/_configs -workflows 16 -out results/audit_<ts>.csv [-grid <dir>[,<dir>...]]
//
// Outputs <out> (one row per combo), <out stem>_templates.csv (one row per
// combo × template) and, with -grid, <out stem>_join.csv (one row per run).
package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/audit"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/config"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/generator"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/seeder"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("audit failed", "err", err)
		os.Exit(1)
	}
}

// comboRe extracts topology, capacity and seed from a grid combo ID such as
// topo_edge_heavy_cap_small_seed3 or topo_uniform_m_cap_large_w_cost_seed1.
var comboRe = regexp.MustCompile(`^topo_(.+?)_cap_(small|medium|large)(?:_w_[a-z]+)?(?:_seed(\d+))?$`)

var tiers = []string{"EDGE", "FOG", "CLOUD"}

type combo struct {
	id, topology, capacity, seed string
	cfg                          *config.Config
	servers                      []*models.Server
	tasks                        []*models.Task
	feas                         map[string]audit.TaskFeasibility
	timing                       audit.Timing
}

func run(args []string) error {
	fs := flag.NewFlagSet("audit", flag.ContinueOnError)
	configsDir := fs.String("configs", "", "directory of per-combo YAML configs (required)")
	nWorkflows := fs.Int("workflows", 16, "workflows to replay per combo")
	drainS := fs.Int("drain", 180, "drain deadline in seconds, for the time bound")
	out := fs.String("out", "", "summary CSV path (default results/audit_<UTC timestamp>.csv)")
	grids := fs.String("grid", "", "comma-separated grid result directories to join with (optional)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	if *configsDir == "" {
		return fmt.Errorf("-configs is required")
	}
	if *out == "" {
		*out = fmt.Sprintf("results/audit_%s.csv", time.Now().UTC().Format("20060102_150405"))
	}
	stem := strings.TrimSuffix(*out, ".csv")

	paths, err := filepath.Glob(filepath.Join(*configsDir, "*.yaml"))
	if err != nil {
		return err
	}
	if len(paths) == 0 {
		return fmt.Errorf("no *.yaml configs in %s", *configsDir)
	}
	sort.Strings(paths)

	taskCache := map[string][]*models.Task{}
	var combos []*combo
	for _, p := range paths {
		c, err := loadCombo(p, time.Duration(*drainS)*time.Second, taskCache)
		if err != nil {
			return fmt.Errorf("%s: %w", filepath.Base(p), err)
		}
		combos = append(combos, c)
	}

	if err := os.MkdirAll(filepath.Dir(*out), 0755); err != nil {
		return err
	}
	if err := writeSummary(*out, combos, *nWorkflows); err != nil {
		return err
	}
	if err := writeTemplates(stem+"_templates.csv", combos); err != nil {
		return err
	}
	fmt.Printf("wrote %s and %s_templates.csv (%d combos)\n", *out, stem, len(combos))
	printBreakdown(combos, *nWorkflows)

	if *grids != "" {
		var dirs []string
		for _, d := range strings.Split(*grids, ",") {
			if d = strings.TrimSpace(d); d != "" {
				dirs = append(dirs, d)
			}
		}
		if err := writeJoin(stem+"_join.csv", dirs, combos); err != nil {
			return err
		}
	}
	return nil
}

func loadCombo(path string, drain time.Duration, cache map[string][]*models.Task) (*combo, error) {
	cfg, err := config.Load(path)
	if err != nil {
		return nil, err
	}
	if cfg.Servers.Seed == 0 || cfg.Generator.Seed == 0 || cfg.Generator.WorkflowSeed == 0 {
		return nil, fmt.Errorf("servers.seed, generator.seed and generator.workflow_seed must be non-zero to audit")
	}
	servers, err := seeder.BuildServers(cfg)
	if err != nil {
		return nil, fmt.Errorf("build servers: %w", err)
	}
	key := fmt.Sprintf("%s|%d|%d", cfg.Generator.DatasetPath, cfg.Generator.TaskCount, cfg.Generator.Seed)
	tasks, ok := cache[key]
	if !ok {
		tasks, err = generator.LoadTasksFromCSV(cfg.Generator.DatasetPath, cfg.Generator.TaskCount, cfg.Generator.Seed)
		if err != nil {
			return nil, err
		}
		cache[key] = tasks
	}
	c := &combo{
		id:      strings.TrimSuffix(filepath.Base(path), ".yaml"),
		cfg:     cfg,
		servers: servers,
		tasks:   tasks,
		feas:    make(map[string]audit.TaskFeasibility, len(tasks)),
		timing: audit.Timing{
			Interval: generator.SubmissionInterval(),
			Window:   time.Duration(cfg.Experiment.DurationSeconds) * time.Second,
			Drain:    drain,
		},
	}
	if m := comboRe.FindStringSubmatch(c.id); m != nil {
		c.topology, c.capacity, c.seed = m[1], m[2], m[3]
	}
	for _, t := range tasks {
		c.feas[t.ID] = audit.Classify(t, servers)
	}
	return c, nil
}

// replay returns the verdicts for the first n workflows of c.
func (c *combo) replay(n int) ([]audit.WorkflowVerdict, error) {
	draws, err := generator.ReplayWorkflows(c.tasks, c.cfg.Generator.WorkflowSeed, c.cfg.Generator.PriorityMin, c.cfg.Generator.PriorityMax, n)
	if err != nil {
		return nil, err
	}
	out := make([]audit.WorkflowVerdict, len(draws))
	for i, d := range draws {
		out[i] = audit.JudgeWorkflow(d, c.feas, c.timing)
	}
	return out, nil
}

type comboStats struct {
	unsched, joint, tooLong              int
	exceeds                              map[string]int
	unschedByTier, templatesByTier       map[string]int
	wfUnsched, wfTime, wfInfeasible, nWF int
}

func (c *combo) stats(n int) (comboStats, error) {
	s := comboStats{exceeds: map[string]int{}, unschedByTier: map[string]int{}, templatesByTier: map[string]int{}}
	deadline := float64(c.timing.Deadline().Milliseconds())
	for _, t := range c.tasks {
		f := c.feas[t.ID]
		s.templatesByTier[t.TargetTier]++
		if !f.Schedulable {
			s.unsched++
			s.unschedByTier[t.TargetTier]++
			if len(f.Exceeds) == 0 {
				s.joint++
			}
			for _, r := range f.Exceeds {
				s.exceeds[r]++
			}
		} else if f.MinEffectiveMS > deadline {
			s.tooLong++
		}
	}
	verdicts, err := c.replay(n)
	if err != nil {
		return s, err
	}
	s.nWF = len(verdicts)
	for _, v := range verdicts {
		if v.HasUnschedulable {
			s.wfUnsched++
		}
		if v.TimeInfeasible {
			s.wfTime++
		}
		if v.Infeasible() {
			s.wfInfeasible++
		}
	}
	return s, nil
}

func writeCSV(path string, header []string, rows [][]string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	if err := w.Write(header); err != nil {
		f.Close()
		return err
	}
	if err := w.WriteAll(rows); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func itoa(i int) string { return strconv.Itoa(i) }

func maxRes(servers []*models.Server) (cpu, mem, disk int) {
	for _, s := range servers {
		cpu, mem, disk = max(cpu, s.MaxCPU), max(mem, s.MaxMemory), max(disk, s.MaxDisk)
	}
	return
}

func writeSummary(path string, combos []*combo, n int) error {
	header := []string{"combo", "topology", "capacity", "seed", "servers_edge", "servers_fog", "servers_cloud",
		"max_cpu", "max_memory_mb", "max_disk_gb", "deadline_s",
		"templates", "unschedulable", "exceeds_cpu", "exceeds_memory", "exceeds_disk", "joint_only",
		"unschedulable_edge", "unschedulable_fog", "unschedulable_cloud",
		"templates_edge", "templates_fog", "templates_cloud", "too_long",
		"workflows", "wf_with_unschedulable", "wf_time_infeasible", "wf_infeasible"}
	var rows [][]string
	for _, c := range combos {
		s, err := c.stats(n)
		if err != nil {
			return fmt.Errorf("%s: %w", c.id, err)
		}
		cpu, mem, disk := maxRes(c.servers)
		rows = append(rows, []string{c.id, c.topology, c.capacity, c.seed,
			itoa(c.cfg.Servers.Edge.Count), itoa(c.cfg.Servers.Fog.Count), itoa(c.cfg.Servers.Cloud.Count),
			itoa(cpu), itoa(mem), itoa(disk), itoa(int(c.timing.Deadline().Seconds())),
			itoa(len(c.tasks)), itoa(s.unsched), itoa(s.exceeds[audit.CPU]), itoa(s.exceeds[audit.Memory]), itoa(s.exceeds[audit.Disk]), itoa(s.joint),
			itoa(s.unschedByTier["EDGE"]), itoa(s.unschedByTier["FOG"]), itoa(s.unschedByTier["CLOUD"]),
			itoa(s.templatesByTier["EDGE"]), itoa(s.templatesByTier["FOG"]), itoa(s.templatesByTier["CLOUD"]), itoa(s.tooLong),
			itoa(s.nWF), itoa(s.wfUnsched), itoa(s.wfTime), itoa(s.wfInfeasible)})
	}
	return writeCSV(path, header, rows)
}

func writeTemplates(path string, combos []*combo) error {
	header := []string{"combo", "task_id", "trace_tier", "cpu", "memory_mb", "disk_gb", "duration_ms",
		"schedulable", "exceeds", "min_effective_ms"}
	var rows [][]string
	for _, c := range combos {
		for _, t := range c.tasks {
			f := c.feas[t.ID]
			minEff := ""
			if !math.IsInf(f.MinEffectiveMS, 1) {
				minEff = strconv.FormatFloat(f.MinEffectiveMS, 'f', 0, 64)
			}
			rows = append(rows, []string{c.id, t.ID, t.TargetTier, itoa(t.CPU), itoa(t.Memory), itoa(t.Disk), itoa(t.Duration),
				strconv.FormatBool(f.Schedulable), strings.Join(f.Exceeds, "+"), minEff})
		}
	}
	return writeCSV(path, header, rows)
}

// printBreakdown prints mean per-combo counts grouped by capacity profile and
// by topology, which is what GATE G1 asks for.
func printBreakdown(combos []*combo, n int) {
	type agg struct {
		k                                           int
		unsched, joint, tooLong, wfU, wfT, wfI, wfN float64
		byTier                                      map[string]float64
	}
	group := func(key func(*combo) string, title string) {
		groups := map[string]*agg{}
		var keys []string
		for _, c := range combos {
			s, err := c.stats(n)
			if err != nil {
				continue
			}
			k := key(c)
			g, ok := groups[k]
			if !ok {
				g = &agg{byTier: map[string]float64{}}
				groups[k] = g
				keys = append(keys, k)
			}
			g.k++
			g.unsched += float64(s.unsched)
			g.joint += float64(s.joint)
			g.tooLong += float64(s.tooLong)
			g.wfU += float64(s.wfUnsched)
			g.wfT += float64(s.wfTime)
			g.wfI += float64(s.wfInfeasible)
			g.wfN += float64(s.nWF)
			for _, t := range tiers {
				g.byTier[t] += float64(s.unschedByTier[t])
			}
		}
		sort.Strings(keys)
		fmt.Printf("\nBy %s (means per combo; workflows = first %d)\n", title, n)
		fmt.Printf("  %-12s %4s  %8s %6s %6s %6s %6s %8s  %8s %8s %8s\n", title, "n", "unsched", "edge", "fog", "cloud", "joint", "too_long", "wf_cap", "wf_time", "wf_any")
		for _, k := range keys {
			g := groups[k]
			d := float64(g.k)
			fmt.Printf("  %-12s %4d  %8.1f %6.1f %6.1f %6.1f %6.1f %8.1f  %8.1f %8.1f %5.1f/%.0f\n", k, g.k,
				g.unsched/d, g.byTier["EDGE"]/d, g.byTier["FOG"]/d, g.byTier["CLOUD"]/d, g.joint/d, g.tooLong/d,
				g.wfU/d, g.wfT/d, g.wfI/d, g.wfN/d)
		}
	}
	group(func(c *combo) string { return c.capacity }, "capacity")
	group(func(c *combo) string { return c.topology }, "topology")
}

// runFile is the subset of an exported result the join needs.
type runFile struct {
	Meta struct {
		Algorithm string `json:"algorithm"`
	} `json:"meta"`
	Workflows []struct {
		TemplateName string    `json:"template_name"`
		Status       string    `json:"status"`
		CreatedAt    time.Time `json:"created_at"`
	} `json:"workflow_results"`
}

// writeJoin matches every run's workflows to replayed draws by creation
// order (rank of created_at) and records how many incomplete workflows the
// audit predicted.
func writeJoin(path string, dirs []string, combos []*combo) error {
	header := []string{"grid", "combo", "algorithm", "file", "wf_total", "wf_done", "wf_incomplete",
		"pred_unschedulable", "pred_time", "pred_infeasible", "pred_infeasible_incomplete",
		"pred_infeasible_but_done", "template_mismatches"}
	var rows [][]string
	var totIncomplete, totExplained, totViolations, totMismatch, nRuns int
	for _, dir := range dirs {
		for _, c := range combos {
			files, _ := filepath.Glob(filepath.Join(dir, c.id, "*.json"))
			sort.Strings(files)
			for _, file := range files {
				data, err := os.ReadFile(file)
				if err != nil {
					return err
				}
				var rf runFile
				if err := json.Unmarshal(data, &rf); err != nil {
					return fmt.Errorf("%s: %w", file, err)
				}
				wfs := rf.Workflows
				sort.SliceStable(wfs, func(i, j int) bool { return wfs[i].CreatedAt.Before(wfs[j].CreatedAt) })
				verdicts, err := c.replay(len(wfs))
				if err != nil {
					return fmt.Errorf("%s: %w", file, err)
				}
				var done, pu, pt, pi, piInc, piDone, mismatch int
				for i, wf := range wfs {
					v := verdicts[i]
					if v.Template != wf.TemplateName {
						mismatch++
					}
					isDone := wf.Status == "DONE"
					if isDone {
						done++
					}
					if v.HasUnschedulable {
						pu++
					}
					if v.TimeInfeasible {
						pt++
					}
					if v.Infeasible() {
						pi++
						if isDone {
							piDone++
						} else {
							piInc++
						}
					}
				}
				nRuns++
				totIncomplete += len(wfs) - done
				totExplained += piInc
				totViolations += piDone
				totMismatch += mismatch
				rows = append(rows, []string{dir, c.id, rf.Meta.Algorithm, filepath.Base(file), itoa(len(wfs)), itoa(done), itoa(len(wfs) - done),
					itoa(pu), itoa(pt), itoa(pi), itoa(piInc), itoa(piDone), itoa(mismatch)})
			}
		}
	}
	if err := writeCSV(path, header, rows); err != nil {
		return err
	}
	share := 0.0
	if totIncomplete > 0 {
		share = 100 * float64(totExplained) / float64(totIncomplete)
	}
	fmt.Printf("\nJoin (%s): %d runs, %d incomplete workflows, %d predicted infeasible (%.1f%% explained); %d predicted-infeasible workflows completed (should be 0); %d template mismatches (should be 0)\n",
		path, nRuns, totIncomplete, totExplained, share, totViolations, totMismatch)
	return nil
}
