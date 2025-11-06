curl -u elastic:elastic \
  -H "Content-Type: application/json" \
  http://localhost:9200/_inference/.elser_model_2/_infer \
  -d '{"input": "warmup"}'
