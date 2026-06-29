#!/usr/bin/env sh
set -eu

existing_tier="$(azd env get-value DEPLOYMENT_TIER 2>/dev/null || true)"
if [ -n "${existing_tier}" ]; then
  echo "DEPLOYMENT_TIER already set to '${existing_tier}'."
  exit 0
fi

echo "Select deployment tier:"
echo "1) try"
echo "2) small"
echo "3) production"
printf "Enter 1, 2, or 3: "
read -r choice

case "${choice}" in
  1) tier="try" ;;
  2) tier="small" ;;
  3) tier="production" ;;
  *) echo "Invalid selection '${choice}'." ; exit 1 ;;
esac

azd env set DEPLOYMENT_TIER "${tier}" >/dev/null
echo "DEPLOYMENT_TIER set to '${tier}'."
