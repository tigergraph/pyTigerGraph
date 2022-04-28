# Preparing LastFM dataset

```bash
7za x data.7z
mv data/*csv .
gsql load_lastfm.gsql  # Upload graph data to TigerGraph
```
