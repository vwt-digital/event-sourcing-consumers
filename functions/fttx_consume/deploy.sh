gcloud functions deploy vwt-d-gew1-fttx-dashboard-consume-func \
  --entry-point=handler \
  --runtime=python37 \
  --trigger-http \
  --project=vwt-d-gew1-fttx-dashboard \
  --region=europe-west1 \
  --timeout=540s
