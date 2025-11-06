#!/usr/bin/env bash

# Prompt the user to pick a Kibana solution view
echo "Select the Kibana solution view:"
echo "1) Elasticsearch"
echo "2) Observability"
echo "3) Security"
echo "4) Classic"
read -rp "Enter the number of your choice [1-4]: " choice

# Map the user's choice to the corresponding solution key
case $choice in
1)
  solution="es"
  ;;
2)
  solution="oblt"
  ;;
3)
  solution="security"
  ;;
4)
  solution="classic"
  ;;
*)
  echo "Invalid choice. Defaulting to 'es' (Elasticsearch)."
  solution="es"
  ;;
esac

# Display the chosen solution
echo "Setting Kibana 'default' space to solution view: $solution"

# Send the update request to Kibana
curl -X PUT "http://localhost:5601/api/spaces/space/default" \
  -H "kbn-xsrf: true" \
  -H "Content-Type: application/json" \
  -u elastic:elastic \
  -d "{
    \"id\": \"default\",
    \"name\": \"Default\",
    \"disabledFeatures\": [],
    \"solution\": \"$solution\"
  }"
