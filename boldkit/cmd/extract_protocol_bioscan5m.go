package cmd

// bioscan5MCurator is the extraction curation protocol scaffold.
// Rule logic and reporting are added in follow-up steps.
type bioscan5MCurator struct {
	cfg extractCurationConfig
}

func newExtractBioscan5MCurator(cfg extractCurationConfig) extractCurator {
	return &bioscan5MCurator{cfg: cfg}
}

func (c *bioscan5MCurator) Curate(*extractTaxonRecord) error {
	return nil
}

func (c *bioscan5MCurator) Close() error {
	return nil
}
