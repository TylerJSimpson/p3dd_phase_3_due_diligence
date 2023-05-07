# Features

## Version 1.0.0

- [x] Job data compiled accurately for companies with <1000 total job openings
- [ ] Accurate FIGI data for companies with Bloomberg Composite Code 'US'
    Requirements:
    - [ ] Algorithm that can create an accurate 'linkedin_source_name' field in figi table
    - [ ] FIGI API call pipeline to populate figi table
    - [ ] Ensure all Bloomberg Exchange Codes are captured (UW, UV, UR, UQ, UP, UN, UF, UA) 
        References:
        - https://stockmarketmba.com/globalstockexchanges.php
- [ ] Analytical views of job data
    Requirements:
    - [ ] Implement dbt for BigQuery warehouse locally on backend machine and integrate with orchestration layer
    - [ ] Create analytical transformations/views for value add
        References:
        - Daily Snapshot
        - Top Companies

## Version 2.0.0

- [ ] Job data compiled accurately for companies with any number of total job openings
- [ ] Job data granularity increased to include job title and job description
- [ ] nct to FIGI data granularity increase to be able to link job data back to individual studies as opposed to companies
- [ ] Accurate FIGI data for companies with and Bloomberg Composite Code
- [ ] Increase granularity of analytical views
- [ ] Move local processes (Prefect, dbt) to cloud VM

## Version 3.0.0
- [ ] Improve velocity of jobs data pipeline from daily to near-real-time
- [ ] Introduce company news data
- [ ] Improve CUSIP/ticker pricing data
- [ ] Company level sentiment analysis implementation