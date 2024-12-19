# This file includes automatic dependency tracking. Please see example of utils
# to see how to add additional files to your project

all: build

BUILD_FLAGS =
ifndef DEV
BUILD_FLAGS += --release
endif

# This is the DB server and client's Unix socket path.
SOCK_PATH = /tmp/cs165_unix_socket_rust

# This is the path to the data persistence directory.
DB_PERSIST_DIR = .cs165_db_rust

build:
	RUSTFLAGS="-C target-cpu=native" \
		SOCK_PATH=$(SOCK_PATH) \
		DB_PERSIST_DIR=$(DB_PERSIST_DIR) \
		cargo build $(BUILD_FLAGS)

clean:
	cargo clean
	rm -f $(SOCK_PATH)

distclean:
	rm -rf $(DB_PERSIST_DIR)

###################### Begin MIRROR DEV TARGETS FROM ROOT ######################

devprep:
	@$(MAKE) --no-print-directory -C .. devprep

devclean:
	@$(MAKE) --no-print-directory -C .. devclean

devlint:
	@$(MAKE) --no-print-directory -C .. devlint

devreaddb:
	@$(MAKE) --no-print-directory -C .. devreaddb

devgen:
	@$(MAKE) --no-print-directory -C .. devgen

devreport:
	@$(MAKE) --no-print-directory -C .. devreport

devserverreport:
	@$(MAKE) --no-print-directory -C .. devserverreport

devtest:
	@$(MAKE) --no-print-directory -C .. devtest

###################### End MIRROR DEV TARGETS FROM ROOT ######################

.PHONY: all clean distclean
