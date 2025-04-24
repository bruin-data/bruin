package lint

type RuleDefinition struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Criteria    string `json:"criteria"`
}

type RuleSet struct {
	Name     string   `json:"name"`
	Selector string   `json:"selector"`
	Rules    []string `json:"rules"`
}

type Specification struct {
	Definitions []RuleDefinition `json:"define"`
	RuleSets    []RuleSet        `json:"rulesets"`
}

func (spec *Specification) Rules() []Rule {
	return nil
}
