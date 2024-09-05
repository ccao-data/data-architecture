#!/usr/bin/env bash
# Publish a message to an SNS topic
#
# Raise on all errors
set -euo pipefail

# Mask topic ARN in GitHub workflow
echo "::add-mask::$1"

# Publish to SNS topic
aws sns publish --topic-arn "$1" --subject "$2" --message "$3"
