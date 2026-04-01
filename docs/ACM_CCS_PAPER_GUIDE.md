# Comprehensive Guide to Writing an ACM CCS 2026 Paper

> ACM Conference on Computer and Communications Security (CCS) is one of the "Big Four"
> top-tier security conferences (alongside IEEE S&P, USENIX Security, and NDSS).
> CCS 2026 is the 33rd edition, held November 15-19, 2026 in The Hague, The Netherlands.

---

## Table of Contents

1. [Key Dates and Deadlines](#1-key-dates-and-deadlines)
2. [Formatting Requirements](#2-formatting-requirements)
3. [Page Limits and Structure](#3-page-limits-and-structure)
4. [Typical Paper Structure](#4-typical-paper-structure-for-security-research)
5. [Tracks and Topics of Interest](#5-tracks-and-topics-of-interest)
6. [Submission Requirements](#6-submission-requirements)
7. [Open Science Policy (Mandatory)](#7-open-science-policy-mandatory)
8. [Ethical Considerations](#8-ethical-considerations)
9. [Generative AI Disclosure Policy](#9-generative-ai-disclosure-policy)
10. [What Reviewers Look For](#10-what-reviewers-look-for)
11. [Best Practices and Tips](#11-best-practices-and-tips-from-successful-authors)
12. [Common Mistakes to Avoid](#12-common-mistakes-to-avoid)
13. [CCS-Specific Requirements vs Other Venues](#13-ccs-specific-requirements-vs-other-venues)
14. [Open Access and Publication](#14-open-access-and-publication)
15. [Resources and Links](#15-resources-and-links)

---

## 1. Key Dates and Deadlines

All deadlines are 11:59 PM AoE (Anywhere on Earth, UTC-12).

### Cycle A (First Round)
| Milestone | Date |
|---|---|
| Abstract registration | January 7, 2026 |
| Full paper submission | January 14, 2026 |
| Author notification | April 9, 2026 |

### Cycle B (Second Round)
| Milestone | Date |
|---|---|
| Abstract registration | April 22, 2026 |
| Full paper submission | April 29, 2026 |
| Online artifact deadline | May 2, 2026 |
| Author notification | July 17, 2026 |

**Important**: Papers rejected in Cycle A **cannot** be resubmitted to Cycle B.

---

## 2. Formatting Requirements

### Template
- Use the **ACM "sigconf" 2-column format** (the `acmart` LaTeX class)
- Document class: `\documentclass[sigconf,anonymous]{acmart}` (for submission)
- The template is available on [Overleaf](https://www.overleaf.com/latex/templates/association-for-computing-machinery-acm-sig-proceedings-template/bmvfhcdnxfty) and from [ACM](https://www.acm.org/publications/proceedings-template)
- Current version: acmart.cls v2.16

### Strictly Prohibited Modifications
The following will trigger **desk rejection without review**:
- Changing fonts from ACM defaults
- Altering margins
- Removing whitespace (e.g., negative `\vspace`, `savetrees` package)
- Removing the author block
- Removing CCS concepts, keywords, or rights management metadata (DOI, ISBN)
- Any other attempts to manipulate formatting to fit more content

### Required Metadata
Even for anonymous submission, retain the following in the template:
- CCS Concepts classification
- Keywords
- Rights management / copyright block (use placeholder values)

---

## 3. Page Limits and Structure

| Component | Page Limit |
|---|---|
| **Main body** (intro through conclusion) | **12 pages maximum** |
| Bibliography | Unlimited (does not count) |
| Appendices (well-marked) | Unlimited (does not count) |
| Open Science appendix (mandatory) | Does not count toward limit |
| Ethical Considerations appendix | Does not count toward limit |
| Generative AI Usage section | Does not count toward limit |
| Supplementary material | Does not count toward limit |

**Critical note**: Reviewers are **not required** to read appendices or supplementary material. Your core contributions must be fully presented within the 12-page body.

---

## 4. Typical Paper Structure for Security Research

While CCS does not mandate a specific section structure, the following is the standard format used by successful CCS papers:

### 4.1 Abstract (~200 words)
- Clearly state the problem, your approach, and key results
- Make it accessible to any security researcher (not just specialists in your sub-area)
- Include quantitative results where possible
- Avoid vague promises; state concrete contributions

### 4.2 Introduction (1.5-2 pages)
- Motivate the problem with a concrete, compelling example
- Clearly state what gap exists in current knowledge/tools
- Summarize your contributions as a bulleted list (typically 3-5 items)
- Briefly preview key results and their significance
- Make it accessible to a general security audience -- any PC member may review your paper

### 4.3 Background / Problem Statement (1-1.5 pages)
- Define the problem formally
- Provide necessary background for readers outside your niche
- Define terms before using them
- Keep this section focused -- don't reproduce entire textbooks

### 4.4 Threat Model (0.5-1 page)
- **Essential for security papers**
- Clearly define the adversary: capabilities, goals, knowledge
- Specify the attack surface and threat boundaries
- State assumptions explicitly (especially implicit ones)
- Explain what is in-scope and out-of-scope
- For ML security papers, this section is **explicitly required** by CCS 2026

### 4.5 System Design / Approach / Methodology (2-3 pages)
- Present your core technical contribution
- Use figures and diagrams to illustrate architecture/workflow
- Explain design decisions and alternatives considered
- Justify why you made the choices you did
- For systems papers: clearly describe the implementation
- For measurement papers: describe data collection methodology

### 4.6 Implementation (0.5-1 page, if applicable)
- Describe prototype/implementation details
- Languages, frameworks, libraries, hardware used
- Lines of code, engineering effort (briefly)
- Enough detail for reproducibility

### 4.7 Evaluation (2-3 pages)
- **This is often the most scrutinized section**
- Clearly state research questions or hypotheses
- Describe experimental setup, datasets, baselines
- Present results with proper statistical methodology
- Use appropriate metrics for your domain
- Compare against state-of-the-art baselines
- Include performance/overhead measurements
- Discuss limitations and threats to validity
- Use tables and figures effectively

### 4.8 Discussion (0.5-1 page)
- Interpret results beyond raw numbers
- Discuss implications and broader impact
- Address limitations honestly
- Suggest future work directions

### 4.9 Related Work (1-1.5 pages)
- Comprehensive survey of directly related work
- Clearly differentiate your work from each cited paper
- Organize thematically, not just chronologically
- Cite recent work (reviewers will check!)

### 4.10 Conclusion (0.5 page)
- Summarize contributions and key findings
- Restate the significance of results
- Brief mention of future directions

### After the 12-Page Body:
- **References** (unlimited)
- **Open Science Appendix** (mandatory -- see Section 7)
- **Ethical Considerations Appendix** (if applicable -- see Section 8)
- **Generative AI Usage Section** (if applicable -- see Section 9)
- **Technical Appendices** (proofs, additional results, etc.)

---

## 5. Tracks and Topics of Interest

CCS 2026 uses a **multi-track format**. You must select one track at abstract registration time. **Track selection cannot be changed after registration.**

Each submission must include a **~200-word justification** in HotCRP explaining why the selected track is the best fit.

### Available Tracks:

1. **Software Security** -- binary analysis, fuzzing, program analysis, malware, vulnerability detection
2. **Web Security** -- browser security, web application security, XSS, CSRF, content security
3. **Network Security** -- protocol security, intrusion detection, DDoS, DNS security, routing security
4. **Security Usability and Measurement** -- user studies, security metrics, ecosystem measurements, security warnings
5. **Security and Privacy of Machine Learning** -- adversarial ML, model privacy, fairness, poisoning, evasion
6. **Formal Methods and Programming Languages** -- verification, type systems, protocol analysis, formal proofs
7. **Hardware, Side Channels, and Cyber Physical Systems** -- microarchitectural attacks, IoT security, SCADA/ICS
8. **Applied Cryptography** -- protocol design, implementation of crypto, secure computation, key management
9. **Blockchain and Distributed Systems** -- smart contract security, consensus, DeFi security
10. **Privacy and Anonymity** -- differential privacy, anonymous communication, data protection, PETs

### Special Note for ML Papers:
- Papers using ML to solve a security problem should go to the **domain-specific track** (e.g., malware detection -> Software Security)
- Only papers directly studying the security/privacy **of** ML itself belong in the ML track
- ML track papers require a clear threat model, actionable security insights, and generalizability across architectures

---

## 6. Submission Requirements

### Anonymization (Double-Blind)
- Remove all author names, affiliations, and identifying information
- Cite your own prior work in the **third person** (e.g., "Smith et al. [5] showed..." not "In our previous work [5]...")
- Do not include deanonymizing GitHub URLs, funding acknowledgments, or institutional references
- If third-person citation is infeasible, use blind references
- **Failure to anonymize properly may result in desk rejection**

### Author Limits
- Maximum **7 papers per author per cycle**
- Author list can be modified between abstract registration and paper submission deadline
- **No authors can be added after paper acceptance**
- Author order may be adjusted in camera-ready version
- All authors must have ORCID IDs registered in HotCRP profiles

### Abstract Registration (Mandatory, 1 Week Before Submission)
- Title, abstract, and track selection are **locked at registration**
- They cannot be modified after the abstract deadline (in HotCRP metadata or the submitted PDF)
- The track justification statement must be substantive (not empty/placeholder)

### Conflict of Interest
Mandatory declared conflicts include:
- Current or former doctoral advisor/advisee
- Members of the same institution
- Close family members
- Co-authors within the past 2 years

### Review Outcomes
- **Accept**: Published in proceedings, presented at conference
- **Minor Revision**: Resubmit within same cycle after addressing reviewer feedback
- **Reject**: Cannot resubmit Cycle A rejections to Cycle B

### Withdrawal Policy
- Papers **cannot be withdrawn** before the final decision notification
- Submitting a full paper commits you to completing the review cycle

---

## 7. Open Science Policy (Mandatory)

**Every** submission must include an "Open Science" appendix (after the bibliography) that:

1. **Enumerates all artifacts** needed to evaluate core contributions (code, datasets, models, configuration files, scripts, documentation, benchmarks)
2. **Describes how reviewers can access each artifact** during double-blind review, including anonymous URLs and credentials
3. **Justifies any artifacts that cannot be shared** (licensing, responsible disclosure, safety, privacy, deployment risks)
4. **States explicitly** if no artifacts are needed

### Artifact Availability Rules
- Artifacts are **mandatory** for papers with implementations, experimental evaluations, systems, tools, or datasets
- Artifacts must be available within **3 days** of paper submission
- For Cycle B: papers using anonymous hosting get a 3-day grace period (until May 2, 2026)
- After the deadline, empty or dubious artifacts at provided URLs trigger **desk rejection**
- Updating artifacts after the deadline triggers **desk rejection**

### Hosting Requirements
**Recommended**: Anonymous hosting services (e.g., [anonymous.4open.science](https://anonymous.4open.science))

**Prohibited/Discouraged**:
- Personal websites, Google Drive, Google Sites (allow tracking/dynamic updates)
- Non-anonymized GitHub repositories
- Zenodo, Figshare (discouraged as default choices)

**For large artifacts (>1GB)**: Provide a representative subset with explanation.

Anonymous URLs must appear both in the paper (Open Science appendix) AND in the HotCRP submission form.

### Optional Artifact Evaluation (Post-Acceptance)
Accepted papers may undergo additional evaluation by the Artifact Evaluation Committee for:
- Functionality badges (code runs, scripts execute, datasets load)
- Reproducibility badges (key results reproducible within reasonable effort)

---

## 8. Ethical Considerations

Papers involving any of the following **must** include an "Ethical Considerations" appendix:
- Human subjects research
- User data collection or analysis
- Real-world vulnerability analysis or disclosure
- Potential for dual-use or harm

This appendix should address:
- Balance of risks versus benefits
- Steps taken to minimize potential harm
- Responsible disclosure procedures followed
- Data anonymization measures
- IRB/ERB approval status (though approval alone is neither necessary nor sufficient)

**When in doubt, include the appendix.** It does not count toward the 12-page limit.

The ethics policy follows the USENIX Security '26 Ethics Policy framework.

---

## 9. Generative AI Disclosure Policy

### Minor Editorial Use (grammar, spelling, style)
- Brief acknowledgment statement sufficient
- Example: "This paper was edited for grammar using [Tool Name]."

### Substantive Content Generation
If AI tools generated or substantially rewrote any content (sentences, paragraphs, code, experiment descriptions), you must provide a dedicated "Generative AI Usage" section that:
- Names the specific tools used
- Describes which parts were generated or heavily assisted
- Explains validation methods applied

### Hard Rules
- AI **cannot** be listed as an author
- **Fabricated citations** (hallucinated references) trigger **desk rejection**
- **Falsified data or results** are treated as **research misconduct** and reported to institutions and ACM

---

## 10. What Reviewers Look For

CCS papers are evaluated on four primary criteria:

### 10.1 Importance to Practice
- Does the work address a real, significant security/privacy problem?
- Will the results matter to practitioners or advance the field?
- Theoretical papers must make a convincing case for practical relevance

### 10.2 Novelty
- Does the paper present genuinely new ideas, not incremental improvements?
- Is the work clearly differentiated from prior art?
- Can you articulate the innovation in 1-2 concise paragraphs?

### 10.3 Quality of Execution
- Is the methodology sound and rigorous?
- Are experiments well-designed with appropriate baselines?
- Are claims supported by evidence?
- Is the evaluation comprehensive (efficiency, accuracy, scalability)?
- Are limitations and threats to validity discussed honestly?

### 10.4 Presentation Quality
- Is the paper well-written and accessible to non-specialists?
- Is the abstract compelling and informative?
- Are figures, tables, and examples clear and helpful?
- Is the paper self-contained within the 12-page body?

### Additional Reviewer Considerations:
- **Threat model clarity**: Is the adversary model well-defined and realistic?
- **Reproducibility**: Can the results be independently verified?
- **Ethical compliance**: Were proper procedures followed?
- **Scope fit**: Does the paper belong at CCS and in the selected track?
- **Responsible disclosure**: For vulnerability papers, was disclosure handled properly?

---

## 11. Best Practices and Tips from Successful Authors

### Writing Strategy

1. **Write the abstract first** -- it forces you to crystallize your contribution and focuses the entire paper.

2. **Make the introduction accessible** -- any PC member may review your paper, so write for a general security audience, not just your sub-field experts.

3. **Lead with the "why"** -- motivate the problem before diving into the "how." Start with a concrete, compelling example of the problem.

4. **State contributions explicitly** -- use a numbered or bulleted list in the introduction. Reviewers scan for this.

5. **Be honest about limitations** -- discussing weaknesses strengthens credibility. Reviewers will find them anyway.

### Technical Content

6. **Define your threat model precisely** -- vague or unrealistic threat models are a common rejection reason, especially for systems security papers.

7. **Choose baselines carefully** -- compare against the strongest, most recent relevant work. Cherry-picking weak baselines is a red flag.

8. **Use proper statistical methodology** -- report confidence intervals, significance tests, and effect sizes. Correct for multiple comparisons.

9. **Explain design alternatives** -- describe what you considered and why you rejected it. This demonstrates deep understanding and saves future researchers from blind alleys.

10. **Distinguish implementation effort from novelty** -- building a large system is impressive but does not by itself constitute a research contribution.

### Practical Tips

11. **Start early on artifacts** -- the Open Science requirement means you need anonymized, accessible artifacts ready at submission time, not as an afterthought.

12. **Use anonymous hosting from the start** -- set up anonymous.4open.science early and test that reviewers can access everything.

13. **Lock your title and abstract before registration** -- they cannot be changed after the abstract deadline. Make them final quality.

14. **Write the track justification carefully** -- the ~200-word statement affects which reviewers see your paper.

15. **Budget your 12 pages wisely** -- allocate roughly: Introduction (1.5-2pp), Background (1-1.5pp), Threat Model (0.5-1pp), Design (2-3pp), Evaluation (2-3pp), Related Work (1-1.5pp), Conclusion (0.5pp).

16. **Read recent CCS proceedings** -- study 5-10 papers from CCS 2024/2025 in your topic area for structure, depth, and presentation patterns. Available at [ACM Digital Library](https://dl.acm.org/doi/proceedings/10.1145/3658644).

---

## 12. Common Mistakes to Avoid

1. **Formatting violations** -- altered margins, fonts, or whitespace cause immediate desk rejection.

2. **Deanonymization leaks** -- GitHub links, funding acknowledgments, or "In our previous work, we..." phrasing.

3. **Missing Open Science appendix** -- every paper needs one, even if you have no artifacts (state that explicitly).

4. **Changing title/abstract after registration** -- the HotCRP metadata and PDF must match what was registered.

5. **Weak or missing threat model** -- especially fatal for systems security and ML security papers.

6. **Unfair evaluation** -- not comparing against state-of-the-art, using weak baselines, or cherry-picking metrics.

7. **Overclaiming** -- making claims stronger than what the evidence supports. Be precise about what you proved.

8. **Ignoring related work** -- missing key references signals lack of awareness. Reviewers in your area will notice.

9. **Writing only for specialists** -- CCS is broad. Any PC member may review your paper. Define jargon.

10. **Fabricated or hallucinated citations** -- if using AI tools, verify every reference exists. Fabricated citations = desk rejection.

11. **Submitting a Cycle A rejection to Cycle B** -- this is explicitly prohibited.

12. **Not having artifacts ready by deadline** -- empty artifact links after the grace period = desk rejection.

13. **Submitting survey/SoK papers** -- CCS explicitly does **not** accept Systematization of Knowledge or survey papers.

---

## 13. CCS-Specific Requirements vs Other Venues

| Feature | CCS 2026 | IEEE S&P 2026 | USENIX Security 2026 | NDSS 2026 |
|---|---|---|---|---|
| Page limit | 12pp + refs/appendices | 13pp + refs/appendices | 13pp + refs/appendices | 13pp + refs |
| Format | ACM sigconf 2-column | IEEE conference | USENIX template | NDSS template |
| Review cycles | 2 (Jan, Apr) | 2 | 3 | 2 |
| SoK papers | **Not accepted** | Accepted | Accepted | Accepted |
| Open Science appendix | **Mandatory** | Not required | Encouraged | Not required |
| Track system | **10 tracks** | Single track | Single track | Single track |
| Track justification | **~200 words required** | N/A | N/A | N/A |
| Artifact requirement | **Mandatory for applicable papers** | Encouraged | Encouraged | Encouraged |
| Title/abstract lock | **At registration (1 week early)** | At submission | At submission | At submission |
| Resubmission of rejections | Cycle A -> Cycle B **prohibited** | Allowed across cycles | Allowed across cycles | Allowed |

### What Makes CCS Unique:
- **Multi-track format**: 10 specialized tracks with dedicated track chairs and PCs
- **Mandatory Open Science appendix**: More rigorous artifact requirements than most venues
- **No SoK/survey papers**: Unlike the other Big Four venues
- **Early title/abstract lock**: Must finalize a week before paper submission
- **Strict formatting enforcement**: Desk rejection for any template modifications
- **Track justification statement**: Unique requirement to explain your track choice

---

## 14. Open Access and Publication

Starting January 1, 2026, ACM has transitioned to 100% Open Access:

- **ACM Open institutional model**: No author-side charges if your institution participates (~1,800 institutions, covering ~70-75% of authors). Check [ACM Open participants](https://libraries.acm.org/acmopen/open-participants).
- **Article Processing Charges (APCs)**: For non-participating institutions:
  - $250 for ACM/SIG members (2026 subsidized rate, 65% discount)
  - $350 for non-members (2026 subsidized rate)
- Waivers and discounts available on a case-by-case basis.

**Publication date**: The official publication date is the first day of the conference, which affects patent filing deadlines.

At least one author of each accepted paper **must register for and attend** the conference in person.

---

## 15. Resources and Links

### Official CCS 2026
- Main site: https://www.sigsac.org/ccs/CCS2026/
- Call for Papers: https://www.sigsac.org/ccs/CCS2026/call-for/call-for-papers.html
- Cycle A HotCRP: https://ccs2026a.hotcrp.com/
- Cycle B HotCRP: https://ccs2026b.hotcrp.com/
- PC Chairs contact: ccs26-pc-chairs@acm.org

### Templates and Formatting
- ACM Primary Article Template: https://www.acm.org/publications/proceedings-template
- Overleaf ACM Template: https://www.overleaf.com/latex/templates/association-for-computing-machinery-acm-sig-proceedings-template/bmvfhcdnxfty
- ACM LaTeX Best Practices: https://www.acm.org/publications/taps/latex-best-practices

### Artifact Hosting
- Anonymous hosting: https://anonymous.4open.science

### Writing Advice
- "How (and How Not) to Write a Good Systems Paper" (Roy Levin & David Redell): https://www.usenix.org/conferences/author-resources/how-and-how-not-write-good-systems-paper
- CCS 2024 Proceedings (study examples): https://dl.acm.org/doi/proceedings/10.1145/3658644

### Reviewing Perspectives
- Andreas Zeller on reviewing across fields (ICSE, PLDI, CCS): https://andreas-zeller.info/2021/07/27/Reviewing-across-fields-ICSE-PLDI-CCS.html
- ACM Guidelines for Evaluating Submissions: https://www.acm.org/publications/policies/pre-publication-evaluation

---

## Quick Checklist Before Submission

- [ ] Paper is 12 pages or fewer (excluding bibliography and appendices)
- [ ] Using unmodified ACM sigconf 2-column template
- [ ] `\documentclass[sigconf,anonymous]{acmart}` specified
- [ ] All author identities removed; self-citations in third person
- [ ] No deanonymizing GitHub links, funding acks, or institutional references
- [ ] Title and abstract finalized (cannot change after registration deadline)
- [ ] Track selected with ~200-word justification written
- [ ] Open Science appendix included with artifact URLs
- [ ] Artifacts hosted on anonymous platform and accessible
- [ ] Ethical Considerations appendix included (if applicable)
- [ ] Generative AI disclosure included (if applicable)
- [ ] All references verified as real (no hallucinated citations)
- [ ] CCS Concepts and Keywords metadata included
- [ ] All authors have ORCID IDs in HotCRP profiles
- [ ] All authors acknowledged submission terms in HotCRP
- [ ] PDF format, properly compiled with no formatting warnings
- [ ] No more than 7 papers submitted by any author in this cycle
