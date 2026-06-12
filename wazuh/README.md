# EduThreat-CTI × Wazuh — SIEM integration

Operationalises the EduThreat-CTI pipeline into a **Wazuh** SIEM in two ways:

- **A. Pipeline observability** — ingest the app's JSON logs and alert on worker
  stalls, fetch-failure spikes, task crashes, and OOM (the SOC/SRE automation of the
  data-quality monitoring otherwise done by hand).
- **B. Threat-intel feed → detections** — the pipeline *produces* CTI (threat actors,
  ransomware families, CVEs, victim countries). Export it as a Wazuh threat feed and
  fire correlation rules when monitored logs match an EduThreat IOC. This is exactly
  how a SOC consumes an external feed — the strongest CTI story here.

This stack is **fully decoupled** from production: it reads logs/exports, runs as a
local Docker stack, and never touches the live worker/api.

```
wazuh/
  docker-compose.yml            single-node Wazuh (manager + indexer + dashboard)
  export_threat_feed.py         dataset -> Wazuh CDB lists (+ STIX 2.1) from the public API
  rules/eduthreat_ops_rules.xml     pipeline observability rules (Angle A)
  rules/eduthreat_intel_rules.xml   IOC-correlation rules (Angle B)
  feeds/                        generated CDB lists + STIX (gitignored)
```

---

## 0. Prerequisites
- Docker + Docker Compose.
- Linux host: `sudo sysctl -w vm.max_map_count=262144` (the indexer needs it; on
  Docker Desktop/macOS it's handled by the VM).

## 1. Bring up the stack
```bash
docker compose -f wazuh/docker-compose.yml up -d
# dashboard: https://localhost:5601   (default admin / SecretPassword)
```
**Change the default password** before anything else (`wazuh-passwords-tool` in the
indexer container, or via the dashboard security UI).

## 2. Generate the threat-intel feed (Angle B)
```bash
python wazuh/export_threat_feed.py --out wazuh/feeds --stix
# -> wazuh/feeds/eduthreat_actors, eduthreat_ransomware_families,
#    eduthreat_cves, eduthreat_victim_countries  (Wazuh CDB lists)
# -> wazuh/feeds/eduthreat_stix_bundle.json       (STIX 2.1, for MISP/OpenCTI)
```
Pulls from the public export API (`/api/v2/export/{dataset}.csv`) — no DB creds.
Override the source with `--base <url>` or `EDUTHREAT_API_BASE`.

Install the lists into the manager and declare them:
```bash
docker cp wazuh/feeds/eduthreat_cves            wazuh.manager:/var/ossec/etc/lists/
docker cp wazuh/feeds/eduthreat_actors          wazuh.manager:/var/ossec/etc/lists/
docker cp wazuh/feeds/eduthreat_ransomware_families wazuh.manager:/var/ossec/etc/lists/
# in /var/ossec/etc/ossec.conf, inside <ruleset>:
#   <list>etc/lists/eduthreat_cves</list>
#   <list>etc/lists/eduthreat_actors</list>
#   <list>etc/lists/eduthreat_ransomware_families</list>
```
Keep the feed fresh as the dataset grows — schedule the exporter (host cron):
```cron
17 * * * *  cd /path/to/EduThreat-CTI && python wazuh/export_threat_feed.py --out wazuh/feeds && \
            docker cp wazuh/feeds/eduthreat_cves wazuh.manager:/var/ossec/etc/lists/ && \
            docker exec wazuh.manager /var/ossec/bin/wazuh-control restart
```

## 3. Install the rules
```bash
docker cp wazuh/rules/eduthreat_ops_rules.xml   wazuh.manager:/var/ossec/etc/rules/
docker cp wazuh/rules/eduthreat_intel_rules.xml wazuh.manager:/var/ossec/etc/rules/
docker exec wazuh.manager /var/ossec/bin/wazuh-control restart
```

## 4. Ship the pipeline's JSON logs (Angle A)
The app already emits one JSON object per line (`LOG_FORMAT=json`, structlog). Two routes:

- **Railway log drain → file → Wazuh** (prod): point a Railway log drain at a small
  collector (or `railway logs --service v2-worker > eduthreat.json.log`) on the host,
  then add it as a localfile of type `json`:
  ```xml
  <localfile>
    <log_format>json</log_format>
    <location>/var/log/eduthreat/worker.json.log</location>
  </localfile>
  ```
- **Local run → file**: run the worker with `LOG_FORMAT=json` redirected to a file and
  point the same localfile at it.

The Wazuh `json` decoder exposes fields as `data.<field>` (`data.event`,
`data.task_type`, `data.level`) — which the ops rules match on.

## 5. Verify the detections
**Ops alert** — simulate a fetch-failure spike:
```bash
for i in $(seq 1 10); do
  echo '{"logger":"src.edu_cti.pipeline.phase2.storage.article_fetcher","event":"FETCH FAILED ALL TIERS","level":"warning"}' \
    >> /var/log/eduthreat/worker.json.log
done
# -> rule 100110 fires per line; 100111 fires once the spike threshold is crossed.
```
**Intel match** — inject a log line containing a dataset CVE/actor:
```bash
echo 'web access: GET /exploit?x=CVE-2019-19781 ...' >> /var/log/monitored/access.log
# -> rule 110100 (EduThreat intel match: known-exploited CVE) fires.
```
Watch results in **Dashboard → Security events**, or tail `/var/ossec/logs/alerts/alerts.json`.

---

## Rule id ranges
- `100100–100199` — pipeline observability (Angle A).
- `110100–110299` — threat-intel correlation (Angle B).
Both inside Wazuh's local custom range; tune levels/thresholds to taste.

## Mapping to skills
SIEM engineering (Wazuh) · detection-rule authoring · log pipeline / JSON ingestion ·
threat-intel feed integration (CDB lists, **STIX 2.1**, MISP-compatible) · IOC lookups ·
SOC alerting & correlation. All backed by a real running pipeline and a live feed
(137 IOCs across actors / ransomware families / CVEs / countries at last export).

## Notes
- The CSV export is the feed's source of truth; the public `/export/*.json` route is
  currently flaky server-side, so the exporter prefers CSV (JSON fallback).
- Nothing here writes to the pipeline or its database. Safe to run anytime.
