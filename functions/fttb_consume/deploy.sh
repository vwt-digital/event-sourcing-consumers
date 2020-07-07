gcloud functions deploy vwt-d-gew1-it-glas-dashboard-consume-func \
  --entry-point=handler \
  --runtime=python37 \
  --trigger-http \
  --project=vwt-d-gew1-it-glas-dashboard \
  --region=europe-west1 \
  --timeout=540s


