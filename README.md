# micrordk-temp-restarts-dashboard

This script prints each restart event on its own line
Restart event lines look like 25 5 test-deadlock 2024-08-24 15:41, where the format is <deviceNum> <Name> <Location> <Date> <Time>

Run as:

```
go run pull.go -api_key_id=<API_KEY_ID> -api_key=<API_KEY> -process-timestamp=true
```

See Viam docs for creating an API key: https://docs.viam.com/dev/tools/cli/#create-an-organization-api-key

I recommend outputting to a file and using pbcopy, since the output is large (3000+ lines for a few months of restart events):

```
go run pull.go -api_key_id=<API_KEY_ID> -api_key=<API_KEY> -process-timestamp=true | tee out
cat out | pbcopy
```
