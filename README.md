🐶 Austin Animal Center Data Analysis

Project Overview

This project analyzes the public intake and outcome data from the Austin Animal Center (AAC) to identify trends, improve operational efficiency, and answer key questions about animal welfare and adoption rates.

The analysis focuses on standardizing data, handling missing values, and deriving new features (e.g., age upon outcome, time spent in the shelter) to inform data-driven decisions.

Methodology and Analysis

1. Data Cleaning and Preprocessing (Python)

Source Data: The primary data comes from two CSV files: Austin_Animal_Center_Intakes.csv and Austin_Animal_Center_Outcomes.csv.  Information regarding specific species was extracted using python and imported into Google sheets where additional information was added regarding group, size, etc.

Initial steps:
	- imported csv files

2. SQL Integration (Future Step)

The cleaned data will be loaded into a SQL database (e.g., PostgreSQL or SQLite) to facilitate complex querying, aggregation, and performance analysis.

3. Key Insights & Findings

(Note: Once analysis is complete, I will replace this text with 3-5 bullet points summarizing what you found. Example topics to explore:)

Adoption Rates by Breed: Which top 5 breeds have the highest and lowest adoption success rates?

Seasonal Trends: Is there a correlation between the time of year and the number of intakes or euthanasia events?

Impact of Spay/Neuter Status: How does the status of an animal upon intake correlate with its eventual outcome?

Dashboard Visualization (Tableau/Power BI)

A publicly viewable dashboard visualizing the final insights will be hosted on [Tableau Public/GitHub Pages].

[LINK TO FINAL DASHBOARD HERE]

 Requirements to Run Code

To replicate the data cleaning steps, please ensure you have the following Python libraries installed:

pandas

numpy

