#!/usr/bin/env bash
set -euo pipefail

TARGET=${1:-hls_fde}

echo "==> Deploying healthcarefinrecon-infra (target: $TARGET)"
pushd healthcarefinrecon-infra
databricks bundle deploy -t "$TARGET"
popd

echo "==> Deploying healthcarefinrecon-app (target: $TARGET)"
pushd healthcarefinrecon-app
databricks bundle deploy -t "$TARGET"
popd

echo "==> Done. Run the rc_recon_deploy job in workspace to execute metric view deployment."
