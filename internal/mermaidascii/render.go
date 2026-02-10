package mermaidascii

import (
	"fmt"
)

// RenderDiagram renders a mermaid diagram from input string.
// If config is nil, default configuration is used.
func RenderDiagram(input string, config *Config) (string, error) {
	if config == nil {
		config = DefaultConfig()
	}

	diag, err := DiagramFactory(input)
	if err != nil {
		return "", fmt.Errorf("failed to detect diagram type: %w", err)
	}

	if err := diag.Parse(input); err != nil {
		return "", fmt.Errorf("failed to parse %s diagram: %w", diag.Type(), err)
	}

	output, err := diag.Render(config)
	if err != nil {
		return "", fmt.Errorf("failed to render %s diagram: %w", diag.Type(), err)
	}

	return output, nil
}
