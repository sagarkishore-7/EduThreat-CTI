# Future Work

Research priorities identified for future development. These are ordered by expected impact on extraction quality and feasibility.

---

## Priority 2: GLiNER Entity Pre-Extraction

**Goal**: Run a lightweight Named Entity Recognition pass over the raw article text before the main LLM extraction call to improve field completeness for institution name, country, city, region, and named threat actors.

**Motivation**: The main LLM extraction step (GBNF-constrained generation) frequently misses location and entity fields when the article is long or the relevant text appears far from the article's start. A fast NER pre-pass can surface these values as hints in the LLM prompt, dramatically reducing null rates for geographic and institutional fields.

**Approach**:
- Use [GLiNER](https://github.com/urchade/GLiNER) — a zero-shot NER model that detects arbitrary entity types via a natural-language entity-type description.
- Run as a preprocessing step in `phase2/extraction/` before the main GBNF call.
- Entity types to extract: `["educational institution", "city", "country", "threat actor", "ransomware family", "government agency"]`
- Feed extracted entities as a structured hint block in the LLM system prompt: `"Entities identified in article: institution=X, city=Y, country=Z"`.
- GLiNER models (e.g. `urchade/gliner_small-v2.1`) run on CPU in ~50ms per article, well within pipeline latency budget.

**Expected impact**: Reduce null rates for `institution_name`, `country`, `city`, `region`, and `ransomware_family` by 30-50% based on benchmark results from the GLiNER paper.

**Files to modify**:
- `src/edu_cti/pipeline/phase2/extraction/` — add `ner_preprocessor.py`
- `src/edu_cti/pipeline/phase2/enrichment.py` — call NER pre-pass before `_call_ollama_extraction()`
- `requirements.txt` — add `gliner>=0.2.0`

---

## Priority 3: IntelEX-Style RAG for MITRE ATT&CK Technique Resolution

**Goal**: Improve MITRE ATT&CK technique extraction quality by grounding LLM responses in a retrieval-augmented generation (RAG) system built from the MITRE ATT&CK knowledge base.

**Motivation**: The current extraction produces `technique_id` values (e.g. `T1566`) but `technique_name`, `tactic`, and `description` are consistently null because the LLM generates them from parametric memory, which is unreliable and depends on training data recency. A RAG approach retrieves the canonical MITRE description for each identified technique ID, ensuring accurate and up-to-date technique metadata without requiring the LLM to recall it.

**Approach**:
- Inspired by [IntelEX](https://arxiv.org/abs/2406.01560) (intelligence extraction with retrieval augmentation).
- Pre-index the MITRE ATT&CK STIX bundle (downloaded from `https://github.com/mitre/cti`) into a local vector store (e.g. ChromaDB or a simple SQLite FTS5 table for dependency-free operation).
- After LLM extraction produces `technique_id` values, look each ID up in the index to retrieve `technique_name`, `tactic`, and `description`.
- Optionally: embed article sentences and retrieve the top-3 most similar MITRE technique descriptions to provide as context during extraction (full RAG).

**Expected impact**: Eliminate the persistent null rate for `technique_name`, `tactic`, and `description` fields. Improve `technique_id` accuracy by providing retrieval-grounded examples in the prompt.

**Files to modify**:
- `src/edu_cti/pipeline/phase2/extraction/` — add `mitre_rag.py`
- `src/edu_cti/pipeline/phase2/enrichment.py` — add post-extraction technique metadata hydration step
- `src/edu_cti/core/` — add MITRE index build script
- `requirements.txt` — add `chromadb>=0.4.0` or use SQLite FTS5 (zero new dependencies)

---

## Priority 4: Multi-Step Pipeline Decomposition

**Goal**: Replace the single-prompt GBNF extraction call with a chain of smaller, focused LLM calls — one per logical extraction group — to improve accuracy on complex incidents.

**Motivation**: The current approach sends one large prompt covering all 40+ fields. Long prompts with complex GBNF grammars increase the probability that the LLM attends to early fields and drops context for later ones. Decomposing into targeted sub-calls (e.g. one call for institution metadata, one for attack dynamics, one for data impact, one for regulatory classification) gives each group the full model attention budget and simpler grammar constraints.

**Approach**:
- Define extraction groups:
  1. **Institution metadata**: `institution_name`, `institution_type`, `country`, `region`, `city`, `sector`
  2. **Attack dynamics**: `attack_category`, `attack_vector`, `attack_chain`, `ransomware_family`, `threat_actor`, `attack_date`
  3. **Data impact**: `data_breached`, `data_categories`, `records_affected_exact`, `records_affected_range`
  4. **Regulatory & response**: `gdpr_breach`, `ferpa_breach`, `hipaa_breach`, `regulatory_body`, `incident_response_steps`, `status`
  5. **MITRE mapping**: `mitre_attack_techniques[]`
- Each group gets its own GBNF grammar constrained to its fields only.
- Results are merged before DB write.
- Groups can potentially run in parallel (asyncio) to reduce wall-clock latency.

**Cost model**: 5 calls × ~4s each = ~20s per incident (vs ~8s for one call). Worthwhile only if field quality improvement is significant; run A/B comparison on a held-out set first.

**Expected impact**: Reduce null rates for `attack_chain`, `timeline.event_description`, and `regulatory_impact` fields — the three fields most persistently null in current production output. These appear near the end of the current large prompt where LLM attention degrades.

**Files to modify**:
- `src/edu_cti/pipeline/phase2/extraction/` — add `decomposed_extractor.py`
- `src/edu_cti/pipeline/phase2/enrichment.py` — add feature flag to route to decomposed vs single-call extractor
- `src/edu_cti/core/config.py` — add `DECOMPOSED_EXTRACTION=False` flag

---

## Not Planned: Priority 5 — vLLM + XGrammar Migration

Migrating from Ollama to vLLM with XGrammar-based grammar-constrained generation would require replacing the entire inference backend and is not feasible within the current infrastructure. Railway deployment uses Ollama via the existing `OllamaLLMClient`; vLLM requires a GPU server with significant RAM. This priority is deferred indefinitely.
