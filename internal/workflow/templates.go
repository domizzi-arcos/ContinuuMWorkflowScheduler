package workflow

import "github.com/pcturnes/distributed-workflow-scheduler/internal/models"

// Templates returns the three canonical workflow DAG definitions used by the
// simulation. Each template exercises a distinct scheduling pattern:
//
//   - pipeline:   linear chain  A→B→C→D
//   - fan_out_in: fork-join     A→{B,C,D}→E
//   - hybrid:     diamond+tail  A→B→{C,D}→E→F
//
// Slots carry no tier constraint. The scheduler decides which server (and
// therefore which tier) executes each task based solely on resource fit and
// the active scheduling algorithm.
func Templates() []models.WorkflowTemplate {
	return []models.WorkflowTemplate{
		pipeline(),
		fanOutIn(),
		hybrid(),
	}
}

// TemplateByName returns the named template, or false if not found.
func TemplateByName(name string) (models.WorkflowTemplate, bool) {
	for _, t := range Templates() {
		if t.Name == name {
			return t, true
		}
	}
	return models.WorkflowTemplate{}, false
}

// pipeline: A → B → C → D (fully serial)
func pipeline() models.WorkflowTemplate {
	return models.WorkflowTemplate{
		Name: "pipeline",
		Slots: []models.WorkflowSlot{
			{ID: "A"},
			{ID: "B", DependsOn: []string{"A"}},
			{ID: "C", DependsOn: []string{"B"}},
			{ID: "D", DependsOn: []string{"C"}},
		},
	}
}

// fanOutIn: A → {B, C, D} → E (fork-join)
func fanOutIn() models.WorkflowTemplate {
	return models.WorkflowTemplate{
		Name: "fan_out_in",
		Slots: []models.WorkflowSlot{
			{ID: "A"},
			{ID: "B", DependsOn: []string{"A"}},
			{ID: "C", DependsOn: []string{"A"}},
			{ID: "D", DependsOn: []string{"A"}},
			{ID: "E", DependsOn: []string{"B", "C", "D"}},
		},
	}
}

// hybrid: A → B → {C, D} → E → F (diamond + tail)
func hybrid() models.WorkflowTemplate {
	return models.WorkflowTemplate{
		Name: "hybrid",
		Slots: []models.WorkflowSlot{
			{ID: "A"},
			{ID: "B", DependsOn: []string{"A"}},
			{ID: "C", DependsOn: []string{"B"}},
			{ID: "D", DependsOn: []string{"B"}},
			{ID: "E", DependsOn: []string{"C", "D"}},
			{ID: "F", DependsOn: []string{"E"}},
		},
	}
}
