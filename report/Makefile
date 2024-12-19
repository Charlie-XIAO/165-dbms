all: data run

# Full data preparation
data:
	./data_tables.py
	./data_dsls.py

# Data preparation for only DSLs (skipping tables)
dsls:
	./data_dsls.py

# Run all experiments (and make the plots)
run:
	./ms1-1.sh
	./ms1-2.sh
	./ms2-1.sh
	./ms2-2.sh
	./ms3-1.sh
	./ms3-2.sh

# Make all plots assuming logs are already generated
plots:
	./ms1-1-plot.py
	./ms1-2-plot.py
	./ms2-1-plot.py
	./ms2-2-plot.py
	./ms3-1-plot.py
	./ms3-2-plot.py

# Clean all generated files
clean:
	rm -rf data logs plots

.PHONY: data dsls run plots clean
