gcloud scheduler jobs delete get-airbnb
gcloud scheduler jobs delete get-events
gcloud scheduler jobs delete get-poi
gcloud scheduler jobs delete compute-recommendations

gcloud scheduler jobs create http get-airbnb --schedule "0 0 * * 1" --time-zone "Europe/Madrid" --uri "https://europe-west1-big-data-keepcoding.cloudfunctions.net/get-airbnb" --http-method POST
gcloud scheduler jobs create http get-events --schedule "0 0 * * *" --time-zone "Europe/Madrid" --uri "https://europe-west1-big-data-keepcoding.cloudfunctions.net/get-events" --http-method POST
gcloud scheduler jobs create http get-poi --schedule "0 0 * * *" --time-zone "Europe/Madrid" --uri "https://europe-west1-big-data-keepcoding.cloudfunctions.net/get-poi" --http-method POST
gcloud scheduler jobs create http compute-recommendations --schedule "0 1 * * *" --time-zone "Europe/Madrid" --uri "https://europe-west1-big-data-keepcoding.cloudfunctions.net/compute-recommendations" --http-method POST
