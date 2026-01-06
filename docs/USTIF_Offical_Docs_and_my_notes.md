Here is the text document formatted for your repository.

---

# PA USTIF ANALYTICAL DOCUMENTATION & INSTITUTIONAL DETAILS

## 1. OFFICIAL DOCUMENTATION INDEX

**A. 2022 Performance Review Report (Aon Global Risk Consulting)**

* **Description:** Statutory 5-year evaluation of fund solvency, claims management performance, and actuarial health. Establishes the baseline "official" view of cost trends.
* **Key Section:** Section 5 ("Performance Review Findings"), specifically "Funding Adequacy."
* **Link:** [https://www.legis.state.pa.us/WU01/LI/TR/Reports/2023_0017R.pdf](https://www.legis.state.pa.us/WU01/LI/TR/Reports/2023_0017R.pdf)

**B. USTIF Claims Manual - Appendix F (TPA Solicitation & Guidelines)**

* **Description:** The operational "playbook" for Third-Party Administrator (ICF) adjusters. Contains the specific checklist used to evaluate claims for auction eligibility.
* **Key Section:** "Pay-for-Performance / Fixed Price Candidate" checklist.
* **Link:** [https://www.emarketplace.state.pa.us/FileDownload.aspx?file=6100060171/Solicitation_6.pdf](https://www.google.com/search?q=https://www.emarketplace.state.pa.us/FileDownload.aspx%3Ffile%3D6100060171/Solicitation_6.pdf)

**C. USTIF Bulletin #5: Competitive Bidding**

* **Description:** Official policy guidance establishing the strategic use of auctions as an intervention tool rather than a default procurement method.
* **Key Section:** Introductory paragraphs outlining "advantageous" use cases.
* **Link:** [https://www.emarketplace.state.pa.us/FileDownload.aspx?file=6100060171/Solicitation_6.pdf](https://www.google.com/search?q=https://www.emarketplace.state.pa.us/FileDownload.aspx%3Ffile%3D6100060171/Solicitation_6.pdf)

**D. PA Code Title 25 § 977.32 (Commonwealth Regulations)**

* **Description:** The specific regulation granting USTIF the statutory authority to force competitive bidding and override tank owner preferences.
* **Key Section:** Subsection (a)(5).
* **Link:** [https://www.pacodeandbulletin.gov/Display/pacode?file=/secure/pacode/data/025/chapter977/s977.32.html](https://www.google.com/search?q=https://www.pacodeandbulletin.gov/Display/pacode%3Ffile%3D/secure/pacode/data/025/chapter977/s977.32.html)

### Evidence for paper:

"The institutional backdrop of the USTIF program is defined by a dichotomy between statutory mandates and operational discretion. While the *2022 Performance Review* (Aon, 2022) characterizes the fund's solvency as robust under current management, it relies on aggregate actuarial projections that mask heterogeneity in claim-level efficiency. Administratively, the program operates under the guidance of *Bulletin #5* (USTIF, 2012), which frames competitive bidding not as a standard procurement requirement, but as a strategic intervention to be deployed when 'advantageous' to the fund. This creates a selective environment where high-cost outliers are systematically culled from the Time-and-Materials pool, generating the survivorship bias evident in raw cost comparisons."

---

## 2. RULES OF ASSIGNMENT (THE "TACIT RULE")

The assignment of claims to the Pay-for-Performance (PFP) auction mechanism is governed by discretionary guidelines rather than rigid algorithmic thresholds.

* **Statutory Authority:** The legal basis is **25 Pa. Code § 977.32(a)(5)**, which states the fund "may require competitive bids for certain work." The use of "may" rather than "shall" creates the legal space for adjuster discretion.
* **Operational Mechanism:** The *USTIF Claims Manual (Appendix F)* provides a qualitative checklist for adjusters. It requires them to assess subjective factors such as "off-site impact," "probable third-party exposure," and the results of "discussion with the claimant."
* **Discretionary Variance:** This framework results in a "Tacit Rule" where assignment depends on the specific interpretation of the Claims Evaluator (adjuster). "Hawk" adjusters may interpret "off-site impact" as an immediate trigger for auction, while "Dove" adjusters may retain such claims on Time-and-Materials contracts.

### Evidence for paper:

"Assignment to the auction treatment is neither random nor determined by a rigid administrative threshold. Instead, it follows a 'Tacit Rule' derived from **25 Pa. Code § 977.32**, which grants the regulator the authority—but not the obligation—to 'require competitive bids.' Operationalized through the *USTIF Claims Manual* checklist, this discretion allows Third-Party Administrator (TPA) agents to interpret site complexity signals heterogeneously. Consequently, the probability of auction assignment () is a function of both site characteristics () and the specific propensity of the assigned agent (), validating the relevance of the adjuster-leniency instrument."

---

## 3. JUDGE-IV DESIGN CONFIRMATION

The institutional structure supports a Judge Instrumental Variable (IV) design.

* **The "Judge":** The Third-Party Administrator (ICF) Claims Evaluators act as the "judges." They possess the effective decision-making power to "sentence" a claim to the auction treatment via the PFP candidacy checklist.
* **Random Assignment:** Claims are typically assigned to evaluators based on geographic territories (DEP Regions) or caseload rotation, independent of the specific future complexity of the remediation. This satisfies the independence assumption ().
* **Exclusion Restriction:** The evaluator's primary lever for influencing cost is the contract type assignment. Provided that "strict" evaluators do not systematically achieve lower costs through other unobserved channels (e.g., better phone negotiation skills) except through the auction mechanism, the exclusion restriction holds.

### Evidence for paper:

"To identify the causal effect of auction procurement, we exploit the quasi-random assignment of claims to ICF Claims Evaluators. Functioning as 'judges' within the administrative hierarchy, these agents exercise significant discretion in triggering the auction mechanism under the guidelines of *Bulletin #5*. We define an instrument  as the leave-one-out mean auction frequency of the evaluator  assigned to claim . Since case assignment is determined by regional workflows rather than site-specific unobservables,  isolates variation in treatment assignment that is orthogonal to site complexity, allowing us to recover the Local Average Treatment Effect (LATE) for marginal claims."