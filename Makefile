# CS165 Staff Note: Generally students do not need to modify this build file
#
# This contains Makefile endpoints/commands for deploying related to docker
# prep/setup/running
#
# NOTE: If you need to make your own targets, add them to the customized section
# at the end. We will periodically update the targets related to staff automated
# testing as the semester progresses. In which case you will need to pull
# upstream from distribution code.

HOST_UID := $(shell id -u)
HOST_GID := $(shell id -g)

# This is where Docker will find your project directory path, only used for
# Docker volume binding purposes
BASE_DIR := $(shell pwd)

# NOTE: IF YOU ARE USING Windows 10 WSL then you need something else in th
# first part of the `pwd` in order to docker bind your Windows (c drive)
# contents to the Windows Docker, uncomment the following, changing what is
# necessary to match your Windows path to your project
# BASE_DIR := "//c/Users/MY_WINDOWS_USERNAME/REST_OF_PATH/cs165-2019-base"

ifdef CS165_PROD
BASE_DIR := $(shell pwd)
endif

# Use C implementation by default; explicitly setting this environment
# variable to 1 will switch to use the Rust implementation
CS165_RUST ?= 0

###################### Begin STAFF ONLY ########################

DOCKER_CMD := docker

all: prep run

# Builds a docker Image called tag `cs165`, from the dockerfile in this current
# directory (`make build`), this target needs to be re-run anytime the base
# dockerfile image is changed you shouldn't need to re-run this much.
build:
	$(DOCKER_CMD) build --tag=cs165 .

# Runs a docker container, based off the `cs165` image that was last built and
# registered kicks off a test bash script and then stops the container.
run:
	$(eval DOCKER_CONT_ID := $(shell $(DOCKER_CMD) container run \
		$(if $(nonroot),--user $(HOST_UID):$(HOST_GID)) \
		-e CS165_RUST=$(CS165_RUST) \
		-v $(BASE_DIR)/src:/cs165/src \
		-v $(BASE_DIR)/rustsrc:/cs165/rustsrc \
		-v $(BASE_DIR)/infra_scripts:/cs165/infra_scripts \
		-v $(BASE_DIR)/project_tests:/cs165/project_tests \
		-v $(BASE_DIR)/generated_data:/cs165/generated_data \
		-v $(BASE_DIR)/student_outputs:/cs165/student_outputs \
		-d --rm -t -i cs165 bash))
	$(DOCKER_CMD) exec $(DOCKER_CONT_ID) bash /cs165/infra_scripts/test.sh
	$(DOCKER_CMD) stop $(DOCKER_CONT_ID)

prep_build:
	$(eval DOCKER_CONT_ID := $(shell cat status.current_container_id | awk '{print $1}'))
	$(DOCKER_CMD) exec $(DOCKER_CONT_ID) bash /cs165/infra_scripts/prep_build.sh

# This endpoint runs a milestone on an already running docker container, based
# off the `cs165` image that was last built and registered. This target is used
# to kick off a test by optionally a milestone #. Note that FS binding is
# one-way, read-only into the docker.
#
# Provide `mile_id` and `restart_server_wait` on the make command line, e.g.
# run milestone 1 and wait 5s between each server restart:
#   make run_mile mile_id=1 server_wait=5
run_mile: prep_build
	@$(eval DOCKER_CONT_ID := $(shell cat status.current_container_id | awk '{print $1}'))
	@$(DOCKER_CMD) exec $(DOCKER_CONT_ID) bash /cs165/infra_scripts/test_milestone.sh $(mile_id) $(server_wait)

# Starts a docker container, based off the `cs165` image that was last built and
# registered. This target is used to kick off automated testing on staff grading
# server. Note that `staff_test` mount point is one-way, read-only into the
# docker.
#
# Usage: make startcontainer outputdir=<ABSOLUTE_PATH1> testdir=<ABSOLUTE_PATH2>
#   ABSOLUTE_PATH1 is the place to output runtime records
#   ABSOLUTE_PATH2 is the place to for reading test cases CSVs, DSLs and EXPs
startcontainer:
	$(eval DOCKER_CONT_ID := $(shell $(DOCKER_CMD) container run \
		$(if $(nonroot),--user $(HOST_UID):$(HOST_GID)) \
		-e CS165_RUST=$(CS165_RUST) \
		-v $(BASE_DIR)/src:/cs165/src \
		-v $(BASE_DIR)/rustsrc:/cs165/rustsrc \
		-v $(BASE_DIR)/infra_scripts:/cs165/infra_scripts \
		-v $(BASE_DIR)/project_tests:/cs165/project_tests \
		-v $(BASE_DIR)/student_outputs:/cs165/student_outputs \
		-v $(testdir):/cs165/staff_test \
		-v $(outputdir):/cs165/infra_outputs \
		-d --rm -t --privileged -i cs165 bash))
	echo $(DOCKER_CONT_ID) > status.current_container_id

# Stops a docker container, the based off the last stored current_container_id.
# If startcontainer was run earlier in a session, it will be stopped by this
# command.
stopcontainer:
	$(eval DOCKER_CONT_ID := $(shell cat status.current_container_id | awk '{print $1}'))
	$(DOCKER_CMD) stop $(DOCKER_CONT_ID)
	rm status.current_container_id

prep:
	[ -d student_outputs ] || mkdir student_outputs
	[ -d generated_data ] || mkdir generated_data

clean:
	rm -rf student_outputs

###################### End STAFF ONLY ##########################

###################### Begin Customization ########################

devprep:
	[ -d outputs ] || mkdir outputs
	[ -d test_data ] || mkdir test_data

devclean:
	rm -rf outputs

# List of C source files under ./src/ to exclude from linting.
LINT_EXCLUDES=include/uthash.h include/utlist.h

# Code formatting and linting. It uses clang-format for C source files,
# black/ruff for Python scripts, and rustfmt/clippy for Rust source files.
devlint:
	@find . \( -name "*.c" -o -name "*.h" \) \
		$(foreach file,$(LINT_EXCLUDES),-not -path "./src/$(file)") | \
		xargs clang-format -i
	@cd rustsrc/ && \
		cargo fmt --all && \
		cargo clippy --fix --allow-dirty --allow-staged -- -D warnings
	@cd project_tests/data_generation_scripts/ && \
		black . && \
		ruff check --select I --fix . && \
		ruff format .
	@cd scripts/ && \
		black . && \
		ruff check --select I --fix . && \
		ruff format .
	@cd report/ && \
		black . && \
		ruff check --select I --fix . && \
		ruff format .

# Read persisted data and print in human-readable format.
devreaddb:
	@python ./scripts/read_persisted.py

# Generate milestone data into the `test_data` directory. `make devgen size=x`
# can control the size (default 10000) of tables generated (except for the two
# dim-tables for join which are always 10000)
devgen: devprep
	@cd project_tests/data_generation_scripts/ && \
		./gen_all_for_staff_use.sh ../../test_data /cs165/staff_test \
			$(if $(size),$(size),10000) 42 10000 10000 $(if $(size),$(size),10000) 1.0 1000

# Check the test cases in the `test_data` directory against the outputs in the
# `outputs` directory, and produce a human-readable report.
devreport:
	@./scripts/compare_tests.sh $(s) $(if $(e),$(e)) | bat

# Listen to stdin for test server results, and produce an interpretation
# automatically copied to the clipboard that can be directly pasted into my
# Excel sheet
devserverreport:
	@./scripts/read_test_server_results.py -o test_server_results.out
	@xclip -selection clipboard test_server_results.out

# Local milestone test. This is a thin wrapper to start a container, run up to
# a certain milestone, and stop the container. The test cases will be taken from
# the `test_data` directory, so make sure to run `make genmiles` first.
devtest:
	@$(MAKE) startcontainer nonroot=1 outputdir=$(BASE_DIR)/outputs testdir=$(BASE_DIR)/test_data
	@$(MAKE) run_mile mile_id=$${m:=5} server_wait=2
	@$(MAKE) stopcontainer
	@$(MAKE) devreport s=1 e=$$( \
		case "$${m:=5}" in \
			1) echo "09";; \
			2) echo "19";; \
			3) echo "44";; \
			4) echo "59";; \
			5) echo "65";; \
		esac \
	)

###################### End Customization ##########################
