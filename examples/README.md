# Preparing LastFM dataset

```bash
7za x data.7z
mv data/*csv .
gsql load_lastfm.gsql  # Upload graph data to TigerGraph
```

# Running recommendation system in Docker container (tgsandbox)

cp -r notebooks/lightGCN_linkpred.ipynb recsys ~/tgsandbox/
# Then open lightGCN_linkpred.ipynb from the file browser in this JupyterLab





