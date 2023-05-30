# Features

## Version 1.0.0

- [x] All phase 3 and late phase 2 clinical trial data compiled and updated daily
- [x] Companies associated with the clinical trials compiled and financial identification maintained automatically (FIGI) for Bloomberg composite code 'US' (companies publicly traded on US market or via ADR)
- [x] Job data compiled daily for companies with <1000 total job openings
- [ ] Analytical models promoted up medallion architecture
    - [ ] More analytical views in development
    - [ ] More operational views in development

## Version 2.0.0

- [ ] Add phase 2 clinical trial data
- [ ] Job data compiled accurately for companies with any number of total job openings
- [ ] Job data granularity increased to include job title and job description
- [ ] nct to FIGI data granularity increase to be able to link job data back to individual studies as opposed to companies
- [ ] Accurate FIGI data for companies with any Bloomberg Composite Code
- [ ] Increase granularity of analytical views to match bronze level granularity increases
- [ ] Move local processes (Prefect, dbt) to cloud VM

## Version 3.0.0
- [ ] Increase velocity of jobs data pipeline from daily to near-real-time
- [ ] Implement company news data
- [ ] Implement stock pricing data
- [ ] Implement company level sentiment analysis
- [ ] Containerize application