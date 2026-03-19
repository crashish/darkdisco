# DarkDisco Findings Triage Report
**Date:** 2026-03-19
**Analyst:** Mayor (automated triage with manual review)
**Period:** 2026-03-17 to 2026-03-19
**Sources:** Telegram Monitor (107 channels), Trapline Integration

---

## Executive Summary

238 findings triaged from `new` status. The majority are Telegram-sourced fraud solicitations targeting financial institutions monitored by DarkDisco. Two high-severity confirmed phishing domains (trapline-sourced) were escalated for takedown investigation.

| Action | Count |
|--------|-------|
| Escalated (high priority) | 2 |
| Confirmed (actionable) | 141 |
| Reviewing (needs manual assessment) | 14 |
| Dismissed (noise) | 83 |

## High-Priority Escalations

### 1. Active Phishing Domain — Suncoast Credit Union
- **Domain:** `suncoastfcu.credit`
- **Source:** Trapline watchlist match, confirmed by OpenPhish
- **Severity:** High
- **Action:** Escalated for takedown investigation
- **Assessment:** Active phishing using `.credit` TLD to impersonate Suncoast FCU. Domain typosquats the real `suncoastfcu.org`. Immediate takedown recommended.

### 2. Active Phishing Domain — VyStar Credit Union
- **Domain:** `vystarcuus.credit`
- **Source:** Trapline watchlist match, confirmed by OpenPhish
- **Severity:** High
- **Action:** Escalated for takedown investigation
- **Assessment:** Same `.credit` TLD pattern as Suncoast finding. Likely same threat actor or phishing kit. Domain impersonates `vystarcu.org`.

## Confirmed Findings by Classification

### Card Fraud (48 findings)
Telegram channels advertising stolen card data, BIN lists, dumps, and cloning services. Most reference specific bank BINs and offer "cashout" services. Key institutions targeted: Global Credit Union, Navy Federal, Commerce Bank.

**Representative example:**
> "SELLING FRESH CC FULLZ [...] BIN 4147 NAVY FEDERAL [...] ALL VERIFIED WITH BALANCE"

### Payment Fraud (31 findings)
Advertisements for fraudulent check/Zelle/wire transfer services. Common pattern: actors offer to process transfers using compromised accounts for a percentage cut.

**Representative example:**
> "ZELLE TRANSFER AVAILABLE [...] NAVY FEDERAL, CHASE, WELLS FARGO [...] 50% SPLIT"

### Mule Recruitment (29 findings)
Direct solicitations for money mules. Actors seek individuals with active bank accounts at specific institutions to receive and forward fraudulent transfers. Typical cut offered: 35-50%.

**Representative example:**
> "IF YOU HAVE NAVY FEDERAL REPORT TO ME 35k INSTANT SAME DAY I DON'T NEED LOGIN"

### Compromised Credentials (11 findings)
Advertising "live logs" (real-time stolen credentials) and stealer log compilations targeting specific institutions.

**Representative example:**
> "Navy Federal CU Log / Balance: $5.9k / LIVE LOG READY"

### Account Trading (10 findings)
Selling access to compromised or synthetic bank accounts ("bank drops"). Accounts advertised as "aged" and "verified" to appear legitimate.

### Identity Fraud (5 findings)
Offering stolen SSNs, driver's licenses, and PII for synthetic identity creation targeting credit unions.

### Fraud Methodology (2 findings)
Sharing "methods" and "tutorials" for committing fraud against specific institutions.

### Phishing Domains (3 additional, low severity)
Three additional domain matches from trapline at low confidence. Confirmed but not escalated.

## Dismissed Findings (83)

Generic keyword matches in Telegram channels where institution names appeared in non-threat contexts (customer service discussions, general banking conversation, channel admin messages). No actionable threat content.

## Institutions Most Targeted

| Institution | Findings | % of Total |
|-------------|----------|------------|
| Global Credit Union | 87 | 37% |
| Navy Federal Credit Union | 78 | 33% |
| United Bank | 31 | 13% |
| Commerce Bank | 12 | 5% |
| 21 other institutions | 30 | 12% |

## Observations and Recommendations

1. **Navy Federal and Global CU are disproportionately targeted.** Both appear across mule recruitment, card fraud, and credential theft. Consider dedicated monitoring channels and increased alerting thresholds.

2. **Telegram is the primary source.** 107/109 pre-triage findings came from Telegram. The channels are a mix of fraud-as-a-service marketplaces and chat groups.

3. **High false positive rate at low severity.** 83/238 (35%) were noise. Recommend:
   - Add negative keyword filters for common benign contexts
   - Require multi-keyword matches (institution name + fraud indicator)
   - Consider semantic analysis vs. pure keyword matching

4. **Trapline integration is highest-signal.** Both escalated findings came from trapline watchlist matches confirmed by OpenPhish. This pipeline produces the most actionable intelligence with lowest noise.

5. **No confirmed phishing pages captured yet.** Browsing sessions have been mostly probing infrastructure targets, not confirmed phishing. As the content signature extraction feature develops, confirmed phishing captures will enable kit identification and campaign tracking.

---

*Report generated from DarkDisco findings database. Classifications applied via pattern matching with manual review of representative samples per category.*
