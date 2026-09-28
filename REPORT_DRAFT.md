# AIMLCZG549 - API-driven Cloud Native Solutions
# Assignment I - ChurnOps: Cloud-Based Customer Churn DataOps Pipeline

**Group ID:** [Group ID]
**Members:** [Member 1], [Member 2]
**Date:** [Submission Date]

---

## 1. Sub-Objective 1: Design and Development of a Data Pipeline

### 1.1 Business Understanding

Telecom companies lose a large chunk of revenue every year to customer
churn, i.e. customers cancelling their subscription and moving to a
competitor. Acquiring a new customer is generally far more expensive than
retaining an existing one, so being able to flag customers who are likely
to churn gives the retention team a chance to step in early with offers or
support before the customer actually leaves.

For this project we picked the Telco Customer Churn dataset since it maps
closely to this problem. It contains the kind of information a retention
team would actually look at - contract type, how long the customer has
stayed, monthly and total charges, which services they use - along with a
label showing whether they churned or not. This makes it a good fit to
build a pipeline around.

### 1.2 Data Ingestion

- Source: Telco Customer Churn dataset (public dataset, originally shared
  by IBM, also available on Kaggle)
- Records: 7,043 customers
- Features: 21 columns covering demographics (gender, SeniorCitizen,
  Partner, Dependents), account details (tenure, Contract,
  PaperlessBilling, PaymentMethod), subscribed services (PhoneService,
  InternetService, OnlineSecurity, TechSupport, StreamingTV, StreamingMovies,
  etc.), billing (MonthlyCharges, TotalCharges) and the target column Churn
- The `load_data()` function in `app.py` reads this CSV using pandas at
  the start of every pipeline run

See the Prefect Cloud logs screenshot in section 1.5, the first line of
every run confirms the dataset loaded correctly with shape (7043, 21).

### 1.3 Data Pre-processing

This is handled in the `preprocess()` step of the pipeline:

| Activity | What we did |
|---|---|
| Summary statistics | `df.describe(include="all")` |
| Missing value check | `df.isnull().sum()`, logged per column |
| Imputation | Numeric columns with nulls are filled with the column median |
| Data types | `df.dtypes` is logged for all 21 columns |
| Normalization | Min-max scaling on the numeric columns - tenure, MonthlyCharges, TotalCharges |

One thing worth noting - `TotalCharges` comes in as a string in the raw
data and has a few blank values for customers with zero tenure, so we
convert it to numeric first and impute the median before it goes into the
rest of the pipeline.

The Prefect Cloud logs screenshot in section 1.5 shows this in action -
missing value counts, the data types for every column, and how many
columns got normalized.

### 1.4 Exploratory Data Analysis (EDA)

The `eda()` step produces four chart images saved to the `artifacts/`
folder:

1. Correlation heatmap (`correlation_heatmap.png`) - shows how tenure,
   MonthlyCharges and TotalCharges correlate with each other.

   [Screenshot: correlation_heatmap.png]

2. Binning - tenure is split into four groups (0-12 months, 13-24 months,
   25-48 months, 49-72 months) to see how churn behavior changes across a
   customer's lifecycle. The bin counts show up in the Prefect logs
   (section 1.5).

3. Encoding / category summary - the top categories for the main
   categorical columns (gender, Partner, Dependents, PhoneService, etc.)
   are logged as a quick summary before label encoding is applied for the
   feature importance model below.

4. Feature importance (`feature_importance.png`) - we trained a Random
   Forest on all the features against Churn to see which ones matter most.
   Tenure, MonthlyCharges, TotalCharges and Contract type came out on top.

   [Screenshot: feature_importance.png]

5. Univariate analysis (`univariate_distributions.png`) - histogram and
   KDE plots for the numeric columns.

   [Screenshot: univariate_distributions.png]

6. Bivariate analysis (`bivariate_analysis.png`) - a boxplot of
   MonthlyCharges against Churn. Customers who churned tend to have
   noticeably higher monthly charges.

   [Screenshot: bivariate_analysis.png]

### 1.5 DataOps - Automating and Scheduling the Pipeline

For the DataOps part of the assignment we used Prefect to turn the
pipeline into a proper scheduled workflow instead of just a script we run
manually.

- The three steps above (`load_data`, `preprocess`, `eda`) are wrapped as
  Prefect tasks inside a single flow called `churn_pipeline()` in `app.py`.
- The flow is deployed on Prefect Cloud with a cron schedule of
  `*/2 * * * *`, so it runs automatically every two minutes.
- Every task logs what it's doing using Prefect's logger - row counts,
  missing value counts, imputation details, normalization, correlation
  results, feature importance - so there's a full activity trail for every
  run without us having to write any custom logging code.
- Prefect Cloud's dashboard shows this run history and the logs for each
  run, which is what we're using as our cloud dashboard for this
  assignment.

[Screenshot 1: Prefect Cloud run history showing several runs completing
about two minutes apart]

[Screenshot 2: Prefect Cloud logs for one run, showing the full sequence
from data loading through preprocessing and EDA]

---

## 2. Sub-Objective 2: API Access

### 3.1 Retrieving Application Details

Alongside the pipeline, `app.py` also defines a FastAPI application that
exposes details about the pipeline and dataset over HTTP. This is deployed
on Render (free tier) at:

`https://customerchurn-whwg.onrender.com`

### 3.2 Application Details Exposed

The assignment asks for at least four application details to be exposed
through the API. We expose:

| # | Endpoint | What it returns |
|---|---|---|
| 1 | GET /status | Status of the most recent pipeline run, with start and finish timestamps |
| 2 | GET /dataset/info | Dataset shape, column names and data types |
| 3 | GET /pipeline/config | Pipeline steps, schedule and which tool orchestrates it |
| 4 | GET /artifacts | List of the EDA chart files generated by the last run |

There are a couple of extra endpoints too - `GET /` for a basic health
check, `GET /artifacts/{filename}` to fetch a specific chart image, and
`POST /pipeline/trigger` to kick off a manual run.

### 3.3 Testing the API

We tested the deployed API directly against the Render URL using a
browser and Postman.

| Endpoint | Method | Expected status | Result |
|---|---|---|---|
| / | GET | 200 | Passed |
| /status | GET | 200 | Passed |
| /dataset/info | GET | 200 | Passed |
| /pipeline/config | GET | 200 | Passed |
| /artifacts | GET | 200 | Passed |
| /runs/nonexistent | GET | 404 | Passed - confirms error handling works |

[Screenshot 1: FastAPI Swagger docs page (/docs) on the deployed Render
URL, listing all the endpoints]

[Screenshot 2: One of the endpoints called directly, showing the JSON
response and a 200 status code]

---

## 3. Architecture

```
GitHub repo (customerchurn)
   |
   |-- app.py              Prefect flow (pipeline) + FastAPI app (API)
   |-- requirements.txt
   |-- data/telco_churn.csv (auto-downloaded if not present)
   |
   +--> Render (free tier)        hosts the FastAPI app, gives us a public API URL
   +--> Prefect Cloud (free tier)  schedules churn_pipeline() every 2 minutes
                                    and acts as our cloud dashboard
```

## 4. Group Member Contributions

| Member | Contribution |
|---|---|
| [Member 1] | [e.g. business understanding, dataset selection, data ingestion, pre-processing and EDA] |
| [Member 2] | [e.g. Prefect pipeline setup, cloud scheduling and dashboard, FastAPI development, API testing and deployment] |

## 5. Conclusion

This project sets up a small but complete cloud-based data pipeline for
customer churn analysis - ingesting the data, cleaning it up, running EDA
on it, and doing all of that on a two-minute schedule through Prefect
Cloud with full logging and a dashboard to monitor it. On top of that, a
deployed and tested FastAPI service exposes the pipeline's details so they
can be accessed over the web, which covers the API access requirement for
this assignment.
