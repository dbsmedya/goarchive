package mermaidascii

import (
	"fmt"
	"strings"
)

func DiagramFactory(input string) (Diagram, error) {
	input = strings.TrimSpace(input)

	if IsSequenceDiagram(input) {
		return &SequenceDiagram{}, nil
	}

	lines := strings.Split(input, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "%%") {
			continue
		}
		if strings.HasPrefix(trimmed, "graph ") || strings.HasPrefix(trimmed, "flowchart ") {
			return &GraphDiagram{}, nil
		}
		if !strings.HasPrefix(trimmed, "%%") {
			return &GraphDiagram{}, nil
		}
	}

	return &GraphDiagram{}, nil
}

type GraphDiagram struct {
	properties *graphProperties
}

func (gd *GraphDiagram) Parse(input string) error {
	properties, err := mermaidFileToMap(input, "cli")
	if err != nil {
		return err
	}
	gd.properties = properties
	return nil
}

func (gd *GraphDiagram) Render(config *Config) (string, error) {
	if gd.properties == nil {
		return "", fmt.Errorf("graph diagram not parsed: call Parse() before Render()")
	}

	if config == nil {
		config = DefaultConfig()
	}

	styleType := config.StyleType
	if styleType == "" {
		styleType = "cli"
	}
	gd.properties.styleType = styleType
	gd.properties.useAscii = config.UseAscii

	return drawMap(gd.properties), nil
}

func (gd *GraphDiagram) Type() string {
	return "graph"
}

// Type returns the diagram type for sequence diagrams.
func (sd *SequenceDiagram) Type() string {
	return "sequence"
}
