# Additional Data Sources for Education Sector Cyber Incidents

This document lists potential additional sources for collecting cyber incident data specific to the education sector.

## Currently Integrated Sources

### Curated Sources (Dedicated Education Sector Sections)
- **KonBriefing** - Curated database of cyber attacks on universities
- **Ransomware.live** - Ransomware leak site aggregator (Education sector filter)
- **DataBreaches.net** - Comprehensive database of data breach incidents

### News Sources (Keyword-Based Search)
- **KrebsOnSecurity** - Cybersecurity news blog
- **The Hacker News** - Cybersecurity news portal
- **The Record** - Cybersecurity news (formerly Recorded Future News)
- **SecurityWeek** - Cybersecurity news and analysis
- **Dark Reading** - Information security news

## Potential Additional Sources

### Government & CERT Sources

1. **CISA K-12 Cybersecurity Initiative**
   - URL: https://www.cisa.gov/k-12-cybersecurity
   - Description: CISA's dedicated resources for K-12 cybersecurity, including alerts and advisories
   - Type: Government alerts, advisories
   - Access: Public RSS feeds, web scraping

2. **CISA Alerts & Advisories**
   - URL: https://www.cisa.gov/news-events/cybersecurity-advisories
   - Description: General cybersecurity advisories that may include education sector incidents
   - Type: Government alerts
   - Access: RSS feeds, API (if available)

3. **NCSC (UK) Education Sector Alerts**
   - URL: https://www.ncsc.gov.uk/section/advice-guidance/all-topics
   - Description: UK National Cyber Security Centre alerts for education sector
   - Type: Government alerts
   - Access: Web scraping, RSS

4. **CERT-EU**
   - URL: https://cert.europa.eu/
   - Description: European Union Computer Emergency Response Team
   - Type: CERT advisories
   - Access: RSS feeds, web scraping

### Academic & Research Sources

5. **Cyber Events Database (University of Maryland)**
   - URL: https://www.cissm.umd.edu/cyber-events-database
   - Description: Structured database of cyber attacks from 2014 to present
   - Type: Research database
   - Access: May require API access or data sharing agreement

6. **CIDAR (NIST)**
   - URL: https://csrc.nist.gov/Projects/cybersecurity-risk-analytics/cidar
   - Description: Cyber Incident Data Analysis Repository
   - Type: Research repository
   - Access: May require data sharing agreement

### News & Media Sources

7. **BleepingComputer**
   - URL: https://www.bleepingcomputer.com/
   - Description: Technology news site with strong cybersecurity coverage
   - Type: News articles
   - Access: Web scraping, RSS feeds
   - Keywords: "university", "school", "education", "ransomware"

8. **Cybersecurity Insiders**
   - URL: https://www.cybersecurity-insiders.com/
   - Description: Cybersecurity news and analysis
   - Type: News articles
   - Access: Web scraping

9. **Infosecurity Magazine**
   - URL: https://www.infosecurity-magazine.com/
   - Description: Information security news and analysis
   - Type: News articles
   - Access: Web scraping, RSS feeds

### Threat Intelligence Platforms

10. **Open Threat Exchange (OTX)**
    - URL: https://otx.alienvault.com/
    - Description: Crowd-sourced threat intelligence platform
    - Type: Threat intelligence
    - Access: API (free tier available)
    - Note: Requires filtering for education sector indicators

11. **MISP Threat Sharing**
    - URL: https://www.misp-project.org/
    - Description: Open-source threat intelligence sharing platform
    - Type: Threat intelligence
    - Access: API, community feeds
    - Note: Requires filtering for education sector

### Ransomware & Leak Sites

12. **RansomWatch**
    - URL: https://ransomwatch.gitlab.io/
    - Description: Ransomware leak site aggregator
    - Type: Leak site aggregator
    - Access: Web scraping, API
    - Note: Filter for education sector victims

13. **RansomFeed**
    - URL: https://ransomfeed.com/
    - Description: Ransomware leak site aggregator
    - Type: Leak site aggregator
    - Access: Web scraping
    - Note: Filter for education sector victims

14. **RansomLook**
    - URL: https://www.ransomlook.io/
    - Description: Ransomware leak site aggregator and monitoring tool
    - Type: Leak site aggregator
    - Access: Web scraping, potentially API
    - Note: Filter for education sector victims

15. **Ransomwhere**
    - URL: https://ransomwhere.com/
    - Description: Ransomware leak site aggregator
    - Type: Leak site aggregator
    - Access: Web scraping
    - Note: Filter for education sector victims

### Data Breach Databases & Reporting Sites

16. **Have I Been Pwned (HIBP) API**
    - URL: https://haveibeenpwned.com/API/v3
    - Description: Database of breached accounts and domains (API available)
    - Type: Data breach database
    - Access: API (free tier, requires API key)
    - Note: Filter for .edu domains and educational institutions
    - Implementation: Query breached domains/emails for education sector

17. **Privacy Rights Clearinghouse (PRC) Chronology of Data Breaches**
    - URL: https://privacyrights.org/data-breaches
    - Description: Chronological database of data breaches since 2005
    - Type: Data breach database
    - Access: Web scraping, downloadable spreadsheets
    - Note: Filter for education sector entries
    - Strength: Historical data going back to 2005

18. **DataBreaches.net**
    - URL: https://www.databreaches.net/
    - Description: Comprehensive data breach news and database
    - Type: Data breach news/database
    - Access: Web scraping, RSS feeds
    - Note: Already partially integrated, may need enhancement

19. **ID Theft Resource Center (ITRC)**
    - URL: https://www.idtheftcenter.org/data-breaches/
    - Description: Non-profit tracking data breaches and identity theft
    - Type: Data breach database
    - Access: Web scraping
    - Note: Filter for education sector incidents

20. **BreachList**
    - URL: https://breachlist.com/
    - Description: Aggregated data breach database
    - Type: Data breach aggregator
    - Access: Web scraping, potentially API
    - Note: Filter for education sector

### Additional News & Media Sources

21. **BleepingComputer** (Already listed, but adding details)
    - URL: https://www.bleepingcomputer.com/
    - Description: Technology news site with strong cybersecurity coverage
    - Type: News articles
    - Access: Web scraping, RSS feeds
    - RSS: https://www.bleepingcomputer.com/feed/
    - Keywords: "university", "school", "education", "ransomware", "college"
    - Priority: HIGH - Very active coverage of ransomware and cyber incidents

22. **Cybersecurity Dive**
    - URL: https://www.cybersecuritydive.com/
    - Description: Industry-focused cybersecurity news
    - Type: News articles
    - Access: Web scraping, RSS feeds
    - Note: Covers sector-specific incidents including education

23. **THE Journal**
    - URL: https://thejournal.com/
    - Description: Education technology news, frequently covers cyber incidents
    - Type: Education tech news
    - Access: Web scraping, RSS feeds
    - Strength: Education-specific perspective

24. **EdTech Magazine**
    - URL: https://edtechmagazine.com/
    - Description: Education technology magazine covering cybersecurity
    - Type: Education tech news
    - Access: Web scraping
    - Note: Covers cybersecurity incidents affecting schools/universities

25. **Inside Higher Ed**
    - URL: https://www.insidehighered.com/
    - Description: Higher education news site covering cyber incidents
    - Type: Higher education news
    - Access: Web scraping, RSS feeds
    - Note: Good coverage of university-specific incidents

26. **Chronicle of Higher Education**
    - URL: https://www.chronicle.com/
    - Description: Major higher education publication
    - Type: Higher education news
    - Access: Web scraping (may require subscription for full articles)
    - Note: Covers significant cyber incidents at universities

27. **ZDNet**
    - URL: https://www.zdnet.com/
    - Description: Technology news with cybersecurity coverage
    - Type: Technology news
    - Access: Web scraping, RSS feeds
    - Keywords: "university", "school", "education", "ransomware"

28. **TechCrunch**
    - URL: https://techcrunch.com/
    - Description: Technology news covering cybersecurity incidents
    - Type: Technology news
    - Access: Web scraping, RSS feeds
    - Note: Occasionally covers major education sector breaches

### Government & Regulatory Sources (Expanded)

29. **CISA Binding Operational Directive (BOD) Reports**
    - URL: https://www.cisa.gov/binding-operational-directives
    - Description: Federal agency cybersecurity directives and reports
    - Type: Government reports
    - Access: Web scraping, RSS feeds
    - Note: May include education sector incidents

30. **EDUCAUSE Cybersecurity Alerts**
    - URL: https://www.educause.edu/
    - Description: Higher education IT association with cybersecurity resources
    - Type: Education sector alerts
    - Access: Web scraping, member resources
    - Note: May require membership for full access

31. **Department of Education - Office of Inspector General Reports**
    - URL: https://www2.ed.gov/about/offices/list/oig/
    - Description: Federal audits and reports on education sector security
    - Type: Government reports
    - Access: Web scraping
    - Note: May include cybersecurity incident reports

32. **State-Level Education Departments**
    - Description: State education departments often report cyber incidents
    - Type: Government reports
    - Access: Web scraping (multiple sources)
    - Examples: State DOE websites, K-12 cybersecurity task forces
    - Note: Requires monitoring multiple state websites

33. **UK Department for Education**
    - URL: https://www.gov.uk/government/organisations/department-for-education
    - Description: UK government education department reports
    - Type: Government reports
    - Access: Web scraping
    - Note: May include UK education sector cyber incident reports

34. **Australian Cyber Security Centre (ACSC) Education Sector Alerts**
    - URL: https://www.cyber.gov.au/
    - Description: Australian government cybersecurity alerts
    - Type: Government alerts
    - Access: Web scraping, RSS feeds
    - Note: Filter for education sector

### Academic Research & Studies

35. **ArXiv - Cybersecurity Research Papers**
    - URL: https://arxiv.org/list/cs.CR/recent
    - Description: Academic papers on cybersecurity incidents
    - Type: Research papers
    - Access: API, web scraping
    - Note: Search for papers mentioning education sector incidents
    - Example: "Understanding Cyber Threats Against the Universities, Colleges, and Schools"

36. **IEEE Xplore Digital Library**
    - URL: https://ieeexplore.ieee.org/
    - Description: Academic papers on cybersecurity
    - Type: Research papers
    - Access: May require institutional access
    - Note: Search for education sector cyber incident studies

37. **Google Scholar**
    - URL: https://scholar.google.com/
    - Description: Academic paper search engine
    - Type: Research papers
    - Access: Web scraping (respect robots.txt)
    - Note: Search for education sector cyber incident case studies

### Industry Reports & Surveys

38. **KnowBe4 Security Awareness Reports**
    - URL: https://www.knowbe4.com/
    - Description: Cybersecurity firm publishing education sector reports
    - Type: Industry reports
    - Access: Web scraping, PDF downloads
    - Note: Regularly publishes education sector threat reports

39. **Verizon Data Breach Investigations Report (DBIR)**
    - URL: https://www.verizon.com/business/resources/reports/dbir/
    - Description: Annual comprehensive data breach report
    - Type: Industry report
    - Access: PDF downloads, data files
    - Note: Includes education sector breakdown, annual publication

40. **Splunk Security Research Reports**
    - URL: https://www.splunk.com/en_us/blog/security.html
    - Description: Security research and threat intelligence
    - Type: Industry reports
    - Access: Web scraping
    - Note: May include education sector threat analysis

41. **Sophos Security Reports**
    - URL: https://www.sophos.com/en-us/security-news-trends/reports
    - Description: Cybersecurity vendor reports
    - Type: Industry reports
    - Access: Web scraping, PDF downloads
    - Note: Includes sector-specific threat reports

42. **ESET Threat Reports**
    - URL: https://www.eset.com/int/about/newsroom/press-releases/
    - Description: Cybersecurity vendor threat intelligence
    - Type: Industry reports
    - Access: Web scraping
    - Note: May include education sector incident analysis

### Specialized Education Cybersecurity Organizations

43. **K12 Security Information Exchange (K12 SIX)**
    - URL: https://www.k12six.org/
    - Description: Information sharing organization for K-12 cybersecurity
    - Type: Information sharing platform
    - Access: May require membership
    - Note: Focused on K-12 sector

44. **REN-ISAC (Research and Education Networks Information Sharing and Analysis Center)**
    - URL: https://www.ren-isac.net/
    - Description: Threat intelligence sharing for higher education and research
    - Type: Information sharing platform
    - Access: Requires membership (typically for .edu institutions)
    - Note: Very relevant but may require institutional membership

45. **Internet2 Security**
    - URL: https://internet2.edu/communities/security/
    - Description: Higher education network organization with security resources
    - Type: Information sharing
    - Access: Web scraping, may require membership
    - Note: Focused on research and education networks

### University IT Status Pages

46. **University IT Status Pages**
    - Description: Many universities maintain public IT status pages that report incidents
    - Type: Official disclosures
    - Access: Web scraping, RSS feeds
    - Examples:
      - Various .edu domains with /status or /it-status pages
      - Status.io pages for universities (e.g., status.iu.edu, status.stanford.edu)
      - Statuspage.io instances
    - Discovery Method: 
      - Search for ".edu status" or "university status page"
      - Check common paths: /status, /it-status, /systems-status
      - Use StatusPage.io public directory
    - Note: Requires discovery and monitoring of multiple domains
    - Strength: First-hand, official incident reports

### Social Media & Forums

47. **Twitter/X - Cybersecurity Researchers**
    - Description: Researchers and journalists often report incidents on social media
    - Type: Social media
    - Access: Twitter API v2 (requires API key, free tier available)
    - Keywords: #ransomware, #databreach, "university", "school", #edutech, #K12cyber
    - Accounts to monitor: @BleepinComputer, @campuscodi, @ransomwaremap, @databreaches
    - Note: Real-time incident reporting, but requires filtering

48. **Reddit - r/cybersecurity, r/ransomware, r/highereducation**
    - Description: Community discussions about cyber incidents
    - Type: Forum discussions
    - Access: Reddit API (requires API key, free tier available)
    - Subreddits: r/cybersecurity, r/ransomware, r/highereducation, r/k12sysadmin
    - Note: Community-sourced incident reports

49. **LinkedIn - Cybersecurity Groups & Posts**
    - Description: Professional network with incident discussions
    - Type: Social media
    - Access: LinkedIn API (requires API key, limited access)
    - Note: Less real-time but may include detailed incident analysis

50. **Mastodon/Fediverse Cybersecurity Instances**
    - Description: Decentralized social network with cybersecurity communities
    - Type: Social media
    - Access: Instance-specific APIs
    - Examples: infosec.exchange, infosec.space
    - Note: Growing alternative to Twitter/X for security researchers

## Implementation Priority

### High Priority (Easy to Integrate) ⭐⭐⭐
**Recommended for immediate implementation:**
1. **BleepingComputer** - Very active coverage, similar structure to existing news sources
2. **CISA K-12 Alerts** - Government RSS feeds, reliable source
3. **RansomWatch** - Similar to ransomware.live, complementary data
4. **THE Journal** - Education-specific news source, good coverage
5. **Privacy Rights Clearinghouse** - Historical data back to 2005, structured format

### Medium Priority (Requires More Work) ⭐⭐
**Valuable but need more development:**
6. **CISA General Advisories** - Need filtering for education sector keywords
7. **NCSC Education Alerts** - UK-specific but valuable for international coverage
8. **Infosecurity Magazine** - News source similar to existing ones
9. **Have I Been Pwned API** - Requires API key, filter for .edu domains
10. **Cybersecurity Dive** - Industry-focused news
11. **Inside Higher Ed** - Higher education specific coverage
12. **ID Theft Resource Center** - Structured breach database
13. **RansomLook / RansomFeed** - Additional ransomware leak site aggregators

### Low Priority (Complex Integration) ⭐
**Require significant effort or access restrictions:**
14. **OTX API** - Requires API key and complex filtering logic
15. **MISP** - Requires community access and filtering
16. **University IT Status Pages** - Requires discovery and monitoring of multiple domains
17. **Social Media (Twitter/X, Reddit)** - Requires API keys and complex filtering/natural language processing
18. **Academic Research Databases (arXiv, IEEE)** - Need to extract incident data from papers
19. **REN-ISAC / K12 SIX** - May require membership for full access
20. **Verizon DBIR** - Annual report, structured but manual integration
21. **Industry Reports** - Periodic publications, need extraction logic

### Specialized Sources (Niche but High Value) 🎯
**Education-sector specific, may have access restrictions:**
- **REN-ISAC** - Premium source but may require institutional membership
- **K12 SIX** - K-12 specific, may require membership
- **EDUCAUSE** - Higher education IT association resources
- **State Education Departments** - Multiple sources, requires aggregation

### Historical & Research Sources 📚
**For comprehensive historical coverage:**
- **Privacy Rights Clearinghouse** - Data back to 2005
- **Academic Research Papers** - Detailed case studies
- **Industry Annual Reports** - Trend analysis and statistics

## Additional Considerations

### Source Characteristics Matrix

| Source Type | Reliability | Detail Level | Update Frequency | Geographic Coverage |
|------------|-------------|--------------|------------------|---------------------|
| Government/CERT | High | Medium | Medium | Country-specific |
| News Sources | Medium-High | High | High | Global |
| Leak Sites | Medium | High (technical) | Very High | Global |
| Research Databases | High | Very High | Low | Varies |
| Social Media | Low-Medium | Medium | Very High | Global |
| Industry Reports | High | High | Low (annual) | Global |

### Integration Challenges & Solutions

1. **Duplicate Detection**: Multiple sources may report the same incident
   - Solution: Use deduplication logic based on institution name, date, incident type
   - Current system has deduplication module - leverage this

2. **Geographic Coverage**: Need international sources for comprehensive dataset
   - Current: Mix of US/UK sources
   - Add: Australian, Canadian, European sources for better coverage

3. **Data Freshness**: Real-time vs. historical coverage
   - Leak sites: Real-time (hours/days)
   - News sources: Near real-time (days)
   - Reports: Delayed (weeks/months)
   - Research: Historical analysis (months/years later)

4. **Access Restrictions**: Some sources require membership or API keys
   - Free tier APIs: HIBP, Twitter, Reddit (with limits)
   - Membership required: REN-ISAC, K12 SIX
   - Public scraping: Most news sites, leak aggregators

### Recommended Implementation Order

**Phase 1 (Quick Wins - 1-2 weeks):**
1. BleepingComputer
2. RansomWatch
3. CISA K-12 Alerts RSS feed

**Phase 2 (High Value - 2-4 weeks):**
4. THE Journal
5. Inside Higher Ed
6. Privacy Rights Clearinghouse
7. Have I Been Pwned API (for .edu domain breaches)

**Phase 3 (Enhanced Coverage - 1-2 months):**
8. Additional ransomware leak aggregators (RansomLook, RansomFeed)
9. ID Theft Resource Center
10. NCSC Education Alerts
11. Cybersecurity Dive

**Phase 4 (Advanced - 2-3 months):**
12. University IT Status Pages (requires discovery infrastructure)
13. Social Media APIs (Twitter/X, Reddit)
14. Industry report parsers (DBIR, etc.)

### Notes

- **All sources should be filtered for education sector relevance** using existing keyword system
- **Consider rate limiting and respectful scraping practices** - follow robots.txt, implement delays
- **Some sources may require API keys or data sharing agreements** - plan for authentication
- **Government sources are typically more reliable** but may have less detail and slower updates
- **News sources provide more context** but may have duplicates with existing sources - deduplication critical
- **Leak sites provide technical details** but may have false positives or unverified claims
- **International sources** are important for comprehensive global coverage
- **Historical sources** (PRC, academic papers) valuable for building complete timeline
- **Education-specific sources** (THE Journal, Inside Higher Ed) provide better context than general security news

