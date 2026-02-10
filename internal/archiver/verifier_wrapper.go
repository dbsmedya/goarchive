package archiver

import (
	"context"
	"database/sql"

	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/dbsmedya/goarchive/internal/types"
	"github.com/dbsmedya/goarchive/internal/verifier"
)

// DataVerifier wraps the verifier package to avoid import cycles.
type DataVerifier struct {
	v *verifier.Verifier
}

// NewDataVerifier creates a new data verifier wrapper.
func NewDataVerifier(source, destination *sql.DB, g *graph.Graph, method string, log *logger.Logger) (*DataVerifier, error) {
	v, err := verifier.NewVerifier(source, destination, g, verifier.VerificationMethod(method), log)
	if err != nil {
		return nil, err
	}
	return &DataVerifier{v: v}, nil
}

// Verify verifies data integrity for discovered records.
func (dv *DataVerifier) Verify(ctx context.Context, recordSet *types.RecordSet) error {
	_, err := dv.v.Verify(ctx, recordSet)
	return err
}
