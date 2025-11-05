SCHEMA=../schema-1.23.json

.PHONY: all \
	atlas-data-lake \
	auth \
	change-streams \
	client-side-encryption \
	client-side-operations-timeout \
	collection-management \
	command-logging-and-monitoring \
	command-logging-and-monitoring/logging \
	command-logging-and-monitoring/monitoring \
	connection-monitoring-and-pooling \
	connection-monitoring-and-pooling/logging \
	crud \
	gridfs \
	index-management \
	load-balancers \
	read-write-concern \
	retryable-reads \
	retryable-writes \
	run-command \
	server-discovery-and-monitoring \
	server-selection \
	server-selection/logging \
	sessions \
	transactions-convenient-api \
	transactions \
	unified-test-format \
	unified-test-format/invalid \
	unified-test-format/valid-fail \
	unified-test-format/valid-pass \
	versioned-api \
	HAS_AJV

all: atlas-data-lake \
	auth \
	change-streams \
	client-side-encryption \
	client-side-operations-timeout \
	collection-management \
	command-logging-and-monitoring \
	connection-monitoring-and-pooling \
	crud \
	gridfs \
	index-management \
	load-balancers \
	read-write-concern \
	retryable-reads \
	retryable-writes \
	run-command \
	server-discovery-and-monitoring \
	server-selection \
	sessions \
	transactions-convenient-api \
	transactions \
	unified-test-format \
	versioned-api

# Keep specifications sorted alphabetically
# When adding a new specification, remember to add it to the all and .PHONY targets above
# For specifications that contain multiple test folders, create a target for each folder
# in addition to a target for the specification itself
atlas-data-lake: HAS_AJV
	@ajv test -s $(SCHEMA) -d "../../atlas-data-lake-testing/tests/unified/*.yml" --valid

auth: HAS_AJV
	@ajv test -s $(SCHEMA) -d "../../auth/tests/unified/*.yml" --valid

change-streams: HAS_AJV
	@ajv test -s $(SCHEMA) -d "../../change-streams/tests/unified/*.yml" --valid

client-side-encryption: HAS_AJV
	@ajv test -s $(SCHEMA) -d "../../client-side-encryption/tests/unified/*.yml" --valid

client-side-operations-timeout: HAS_AJV
	@ajv test -s $(SCHEMA) -d "../../client-side-operations-timeout/tests/*.yml" --valid

collection-management: HAS_AJV
	@ajv test -s $(SCHEMA) -d "../../collection-management/tests/*.yml" --valid

command-logging-and-monitoring: command-logging-and-monitoring/logging command-logging-and-monitoring/monitoring

command-logging-and-monitoring/logging: HAS_AJV
	@ajv test -s $(SCHEMA) -d "../../command-logging-and-monitoring/tests/logging/*.yml" --valid

command-logging-and-monitoring/monitoring: HAS_AJV
	@ajv test -s $(SCHEMA) -d "../../command-logging-and-monitoring/tests/monitoring/*.yml" --valid

connection-monitoring-and-pooling: connection-monitoring-and-pooling/logging

connection-monitoring-and-pooling/logging: HAS_AJV
	@ajv test -s $(SCHEMA) -d "../../connection-monitoring-and-pooling/tests/logging/*.yml" --valid

crud: HAS_AJV
	@ajv test -s $(SCHEMA) -d "../../crud/tests/unified/*.yml" --valid

gridfs: HAS_AJV
	@ajv test -s $(SCHEMA) -d "../../gridfs/tests/*.yml" --valid

index-management: HAS_AJV
	@ajv test -s $(SCHEMA) -d "../../index-management/tests/*.yml" --valid

load-balancers: HAS_AJV
	@ajv test -s $(SCHEMA) -d "../../load-balancers/tests/*.yml" --valid

read-write-concern: HAS_AJV
	@ajv test -s $(SCHEMA) -d "../../read-write-concern/tests/operation/*.yml" --valid

retryable-reads: HAS_AJV
	@ajv test -s $(SCHEMA) -d "../../retryable-reads/tests/unified/*.yml" --valid

retryable-writes: HAS_AJV
	@ajv test -s $(SCHEMA) -d "../../retryable-writes/tests/unified/*.yml" --valid

run-command: HAS_AJV
	@ajv test -s $(SCHEMA) -d "../../run-command/tests/unified/*.yml" --valid

server-discovery-and-monitoring: HAS_AJV
	@ajv test -s $(SCHEMA) -d "../../server-discovery-and-monitoring/tests/unified/*.yml" --valid

server-selection: server-selection/logging

server-selection/logging: HAS_AJV
	@ajv test -s $(SCHEMA) -d "../../server-selection/tests/logging/*.yml" --valid

sessions: HAS_AJV
	@ajv test -s $(SCHEMA) -d "../../sessions/tests/*.yml" --valid

transactions-convenient-api: HAS_AJV
	@ajv test -s $(SCHEMA) -d "../../transactions-convenient-api/tests/unified/*.yml" --valid

transactions: HAS_AJV
	@ajv test -s $(SCHEMA) -d "../../transactions/tests/unified/*.yml" --valid

unified-test-format: unified-test-format/invalid unified-test-format/valid-fail unified-test-format/valid-pass

unified-test-format/invalid: HAS_AJV
	@# Redirect stdout to hide expected validation errors
	@ajv test -s $(SCHEMA) -d "invalid/*.yml" --invalid > /dev/null && echo "invalid/*.yml passed test"

unified-test-format/valid-fail: HAS_AJV
	@ajv test -s $(SCHEMA) -d "valid-fail/*.yml" --valid

unified-test-format/valid-pass: HAS_AJV
	@ajv test -s $(SCHEMA) -d "valid-pass/*.yml" --valid

versioned-api: HAS_AJV
	@ajv test -s $(SCHEMA) -d "../../versioned-api/tests/*.yml" --valid

HAS_AJV:
	@if ! command -v ajv > /dev/null; then                \
	    echo 'Error: need "npm install -g ajv-cli"' 1>&2; \
	    exit 1;                                           \
	fi
