\.PHONY: clean create_environment test

#################################################################################
# GLOBALS                                                                       #
#################################################################################

PROJECT_NAME = pipeline-oriented-analytics

# A wrapper to executre commands in conda environment
define execute_in_env
	source activate $(PROJECT_NAME); \
	$1
endef

#################################################################################
# COMMANDS                                                                      #
#################################################################################

## Delete conda environment
clean:
	conda env remove -n $(PROJECT_NAME)

## Create conda environment
init:
	conda create  -n $(PROJECT_NAME) python=3.6 -y
	$(call execute_in_env, pip install -r requirements-dev.txt)

## Run tests
test:
	$(call execute_in_env, python -m pytest tests/ )

## Generate distance matrix
dm:
	$(call execute_in_env, python src/pipeline_oriented_analytics/script/generate_distance_matrix.py)

## Pre-process train set
ppt:
	$(call execute_in_env, python src/pipeline_oriented_analytics/script/pre_process.py train)

## Pre-process test set
ppp:
	$(call execute_in_env, python src/pipeline_oriented_analytics/script/pre_process.py predict)

## Extract train features
eft:
	$(call execute_in_env, python src/pipeline_oriented_analytics/script/extract_features.py train)

## Extract predict features
efp:
	$(call execute_in_env, python src/pipeline_oriented_analytics/script/extract_features.py predict)

## Train model
train:
	$(call execute_in_env, python src/pipeline_oriented_analytics/script/train.py)

## Make predictions
predict:
	$(call execute_in_env, python src/pipeline_oriented_analytics/script/predict.py)

#################################################################################
# Self Documenting Commands                                                     #
#################################################################################

.DEFAULT_GOAL := help

# Inspired by <http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html>
# sed script explained:
# /^##/:
# 	* save line in hold space
# 	* purge line
# 	* Loop:
# 		* append newline + line to hold space
# 		* go to next line
# 		* if line starts with doc comment, strip comment character off and loop
# 	* remove target prerequisites
# 	* append hold space (+ newline) to line
# 	* replace newline plus comments by `---`
# 	* print line
# Separate expressions are necessary because labels cannot be delimited by
# semicolon; see <http://stackoverflow.com/a/11799865/1968>
.PHONY: help
help:
	@echo "$$(tput bold)Available rules:$$(tput sgr0)"
	@echo
	@sed -n -e "/^## / { \
		h; \
		s/.*//; \
		:doc" \
		-e "H; \
		n; \
		s/^## //; \
		t doc" \
		-e "s/:.*//; \
		G; \
		s/\\n## /---/; \
		s/\\n/ /g; \
		p; \
	}" ${MAKEFILE_LIST} \
	| LC_ALL='C' sort --ignore-case \
	| awk -F '---' \
		-v ncol=$$(tput cols) \
		-v indent=19 \
		-v col_on="$$(tput setaf 6)" \
		-v col_off="$$(tput sgr0)" \
	'{ \
		printf "%s%*s%s ", col_on, -indent, $$1, col_off; \
		n = split($$2, words, " "); \
		line_length = ncol - indent; \
		for (i = 1; i <= n; i++) { \
			line_length -= length(words[i]) + 1; \
			if (line_length <= 0) { \
				line_length = ncol - indent - length(words[i]) - 1; \
				printf "\n%*s ", -indent, " "; \
			} \
			printf "%s ", words[i]; \
		} \
		printf "\n"; \
	}' \
	| more $(shell test $(shell uname) = Darwin && echo '--no-init --raw-control-chars')


