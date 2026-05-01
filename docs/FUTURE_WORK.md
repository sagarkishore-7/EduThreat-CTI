# Future Work

Research priorities for EduThreat-CTI. All originally planned priorities have been implemented.

---

## Priority 2: GLiNER Entity Pre-Extraction ✅ Implemented (v2.9.0, 2026-05-01)

**Goal**: Run a lightweight Named Entity Recognition pass over the raw article text before the main LLM extraction call to improve field completeness for institution name, country, city, region, and named threat actors.

**Implementation**: `src/edu_cti/pipeline/phase2/extraction/ner_preprocessor.py`

- Model: `urchade/gliner_small-v2.1` (zero-shot, CPU, ~150 MB)
- Entity types: educational institution, city, country, US state/Canadian province, threat actor group, ransomware family
- Output: `=== NER PRE-EXTRACTION HINTS ===` block injected into the LLM user prompt in both `_enrich_article()` and `_enrich_article_split()`
- Model cached to `DATA_DIR/hf_cache` via `HF_HOME` override so it survives Railway container restarts
- Gracefully no-ops if GLiNER unavailable or model load fails

**Observed behaviour**: Correctly extracts institution names, city/state pairs, and ransomware family names from realistic article text. Total prompt overhead ~500–800 chars.

---

## Priority 3: IntelEX-Style RAG for MITRE ATT&CK ✅ Implemented (v2.9.0, 2026-05-01)

**Goal**: Improve MITRE ATT&CK technique extraction quality by grounding LLM responses in a retrieval-augmented generation system built from the MITRE ATT&CK knowledge base.

**Implementation — two components**:

### 3a. STIX Technique Lookup (`mitre_stix.py`)

Post-extraction hydration of `technique_name`, `tactic`, and `description` fields from the authoritative MITRE ATT&CK STIX bundle:

- Downloads the Enterprise STIX bundle from `github.com/mitre/cti` (697 active techniques)
- Caches to `DATA_DIR/mitre_attack_cache.json`, refreshed every 30 days
- `get_technique_info(id)` returns `{name, tactic, description}` with subtechnique → base fallback
- Phase-name normalisation: `"stealth"` → `"Defense Evasion"`, `"initial-access"` → `"Initial Access"`, etc.
- Wired into `_fill_mitre_technique_names()` in `post_processing.py` as primary lookup; static dict remains as secondary fallback

### 3b. Semantic RAG (`mitre_rag.py`)

Pre-extraction retrieval of the most relevant MITRE techniques as prompt context, so the LLM selects technique IDs from evidence rather than memory:

- Embeds all 697 technique descriptions with `all-MiniLM-L6-v2` (384-dim, ~90 MB)
- Embeddings cached to `DATA_DIR/mitre_embeddings.npy` + `mitre_embeddings_index.json`
- `retrieve_similar_techniques(article_text, top_k=5)` finds candidates via normalised cosine similarity in ~10ms on CPU
- `build_mitre_rag_block()` formats a `=== MITRE ATT&CK CANDIDATE TECHNIQUES ===` context block
- Injected into Part 2 prompt in `_enrich_article_split()` and into the single-call prompt in `_enrich_article()`
- Inspired by IntelEX (arxiv 2406.01560)

---

## Priority 4: Multi-Step Pipeline Decomposition ✅ Implemented (v2.7.0 / v2.8.0)

**Goal**: Replace the single-prompt GBNF extraction call with a chain of smaller, focused LLM calls to improve accuracy on complex incidents.

**Implementation**: `_enrich_article_split()` in `enrichment.py`

- **Part 1** — core identification: institution metadata, attack classification, attack vector, data impact (~45 fields, `EXTRACTION_SCHEMA_PART1`)
- **Part 2** — deep intelligence: MITRE ATT&CK (all 4 fields), timeline with event descriptions, regulatory impact, financial details (~35 fields, `EXTRACTION_SCHEMA_PART2`). Now also receives the IntelEX RAG block for grounded technique selection.
- **Part 3** — enriched summary generation
- Parts 1 + 2 merged before DB write; Part 2 overwrites where populated
- Falls back to single-call `_enrich_article()` if Part 1 fails

---

## Priority 5: vLLM + XGrammar Migration — Deferred Indefinitely

Migrating from Ollama to vLLM with XGrammar-based grammar-constrained generation requires a GPU server with significant RAM. Railway deployment uses Ollama on CPU; vLLM is not feasible in the current infrastructure. This priority remains deferred until a GPU-equipped inference server is available.

---

## Potential Future Directions

These were not part of the original plan but are worth considering as the dataset grows:

- **Retroactive re-enrichment with GLiNER + RAG** — run `POST /api/admin/re-enrich` on existing incidents to backfill technique names and improve institution/location fields using the new grounding. High impact, low risk (post-processing only fills null fields).

- **GLiNER fine-tuning on education-sector annotations** — the small model occasionally misidentifies acronyms (e.g. "LAUSD" detected correctly but "MPS" or "HISD" may not be). A small labelled set of edu-sector NER examples could fine-tune the model for higher precision.

- **Expand RAG to non-MITRE fields** — the same semantic retrieval pattern could be applied to attack vector classification (retrieve from a labelled attack vector description corpus) or regulatory framework selection (retrieve from GDPR/FERPA/HIPAA rule descriptions).

- **Confidence scores for retrieved techniques** — currently the top-5 are all injected regardless of score. Adding a minimum similarity threshold (e.g. 0.35) would filter irrelevant retrievals for articles about non-technical incidents (policy announcements, awareness campaigns).
