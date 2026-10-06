# CHRIST COLLEGE OF ENGINEERING (AUTONOMOUS), IRINJALAKUDA
## Department of Computer Science and Engineering (Data Science)

### Project Abstract
**Subjects:** Data Analytics and Big Data Processing

| Group                 | Group 2                             |
| --------------------- | ----------------------------------- |
| Domain                | Healthcare / Medical Data Analytics |
| Topic Submission Date | 21/09/2026                          |

| Sl. No. | Name of Student | Register No. |
| ------- | --------------- | ------------ |
| 1       | Alvi A V        | CCE24CD009   |
| 2       | Ayush P Shibu   | CCE24CD025   |
| 3       | Minhaj Noushad  | CCE24CD043   |
| 4       | Joyal Shinoy    | CCE24CD046   |
| 5       | Thomas Robin    | CCE24CD059   |

---

## 1. Project Title

**Scalable Big Data Analytics Framework for Real-Time Health-Risk Monitoring Using Multivariate Patient Vital-Sign Data**

---

## 2. Problem Statement

Modern patient-monitoring systems, whether in hospital wards, intensive care units, or wearable/IoT-based remote monitoring setups, continuously generate large volumes of multivariate physiological data such as heart rate, SpO₂, respiratory rate, body temperature, blood pressure, and ECG-derived features. This data exhibits the classical characteristics of a Big Data problem: high volume due to continuous multi-parameter recording across many patients, high velocity because vital-sign streams are generated at regular, often sub-minute, intervals, and high variety because measurements differ in scale, sampling frequency, and unit across physiological channels.

Conventional single-machine, batch-oriented processing pipelines are not well suited to handle datasets of this scale and continuity. As the number of monitored patients and the duration of monitoring increase, single-node storage and processing quickly become a bottleneck, both in terms of storage capacity and computation time. Furthermore, because vital-sign data is inherently time-series and multivariate in nature, identifying clinically relevant temporal patterns, abnormal physiological events, and gradual deterioration trends requires analytical techniques that go beyond simple threshold-based alerts.

There is a clear technical gap between the raw volume of continuously generated physiological data and the availability of scalable, distributed analytics pipelines capable of transforming this raw sensor data into interpretable, decision-support-oriented risk indicators. Publicly available ICU monitoring archives, such as PhysioNet’s MIMIC-III Waveform Database, already illustrate this scale problem: tens of thousands of patient records containing dense, minute-by-minute vital-sign time series that quickly outgrow single-machine tools once multiple patients, signals, and monitoring days are combined. This project addresses that gap by proposing a distributed Big Data processing and analytics pipeline that can ingest, store, process, and analyze large-scale multivariate patient vital-sign data, combining both historical/batch analysis and real-time streaming analytics, while producing interpretable risk and anomaly indicators rather than raw, unprocessed readings.

---

## 3. Motivation

The increasing adoption of continuous and remote patient monitoring, driven by wearable devices, bedside monitors, and IoT-enabled sensors, has made large-scale physiological data increasingly available for analysis. Early identification of abnormal physiological patterns can support timely clinical attention, and scalable analytics pipelines are necessary to process this continuously growing data efficiently.

This project is motivated by the need to combine descriptive, diagnostic, and predictive analytics into a single coherent pipeline capable of operating at scale. Descriptive analytics allows patient trends to be summarized and understood; diagnostic analytics helps investigate relationships between physiological variables and abnormal events; and predictive analytics supports early anomaly and risk detection. Equally important is the need for interpretable outputs, since raw model scores are of limited use to healthcare professionals without an understanding of which physiological factors contributed to a given risk assessment.

The project is intended as an academic exploration of scalable healthcare data analytics and is designed as a decision-support and monitoring aid for healthcare professionals. It does not claim to diagnose disease or replace clinical judgment.

---

## 4. Objectives

1. Collect and integrate multivariate patient vital-sign time-series data from suitable publicly available healthcare/physiological datasets.
2. Design a scalable Big Data architecture appropriate for multivariate healthcare time-series data.
3. Implement distributed storage of the dataset using the Hadoop Distributed File System (HDFS).
4. Use MapReduce and/or Hive for large-scale batch processing and SQL-based querying of stored data.
5. Use Apache Spark / PySpark for distributed, in-memory data processing and analytics.
6. Use Apache Kafka for streaming data ingestion, where feasible, to simulate continuous vital-sign generation.
7. Use Spark Structured Streaming for real-time analytics on the ingested data stream, where feasible.
8. Perform data cleaning, missing-value handling, normalization, and preprocessing of the physiological data.
9. Perform temporal and statistical feature engineering (e.g., rolling statistics, trend and variability features) on the time-series data.
10. Develop descriptive analytics to summarize patient vital-sign trends and distributions.
11. Develop diagnostic analytics to investigate relationships and correlations between physiological variables and abnormal patterns.
12. Develop predictive and anomaly-detection models to support health-risk monitoring.
13. Compare the performance of multiple suitable machine-learning models for the selected task.
14. Apply explainability techniques such as SHAP to interpret model outputs and risk scores.
15. Store processed and risk-related results using HBase or another appropriate technology for efficient retrieval.
16. Develop an interactive dashboard to visualize patient trends, anomalies, and risk indicators.
17. Empirically compare distributed processing with conventional single-machine processing using measurable metrics such as processing time, throughput, scalability, and latency.

---

## 5. Proposed Solution

The proposed system follows an end-to-end distributed data pipeline that transforms raw, multivariate patient vital-sign data into interpretable health-risk indicators. The high-level architecture is as follows:

```
Patient/Vital-Sign Data Sources
        ↓
Data Ingestion
        ↓
Kafka Streaming Layer (for simulated real-time ingestion)
        ↓
HDFS Distributed Storage
        ↓
MapReduce / Hive Batch Processing
        ↓
Apache Spark / PySpark Processing
        ↓
Spark SQL Querying
        ↓
Data Cleaning and Preprocessing
        ↓
Temporal and Statistical Feature Engineering
        ↓
Batch and Streaming Analytics
        ↓
Machine Learning / Anomaly Detection
        ↓
Risk and Anomaly Scoring
        ↓
HBase Analytical Storage
        ↓
Interactive Dashboard
```

The primary data source proposed for this project is the **MIMIC-III Waveform Database**, a publicly available PhysioNet resource containing numerics records (quasi-continuous time series of vital signs such as heart rate, SpO₂, respiration, and blood pressure) for approximately 30,000 ICU patients across 67,830 record sets. Where broader coverage or complementary signals are useful, this may be supplemented with other credentialed PhysioNet resources such as the eICU Collaborative Research Database or VitalDB. Access to PhysioNet’s credentialed datasets requires completing the required data-use training and agreement, and the exact subset used will be finalized once access is confirmed; the project design remains feasible using any one of these sources should another become unavailable.

Each component of this architecture serves a distinct, justified purpose rather than being included for the sake of technological breadth:

- **HDFS** provides fault-tolerant, distributed storage capable of scaling with the volume of accumulated vital-sign records across many patients and monitoring sessions.
- **MapReduce and Hive** support large-scale batch processing and SQL-style querying of historical data, enabling efficient exploratory and descriptive analysis over the full dataset.
- **Kafka** enables continuous, decoupled ingestion of vital-sign records, simulating the arrival pattern of a real-time patient-monitoring stream.
- **Spark and PySpark** provide distributed, in-memory computation for feature engineering, batch analytics, and machine-learning workflows at scale, substantially reducing processing time compared to single-machine equivalents.
- **Spark SQL** enables structured, large-scale querying of processed data for both descriptive and diagnostic analytics.
- **Spark Structured Streaming** enables real-time analytics such as sliding/tumbling window aggregation and continuous anomaly scoring on the ingested stream.
- **HBase** offers low-latency, random-access storage suited to retrieving individual patient or time-window risk records for the dashboard.

The analytics layer is organized into four distinguishable levels:

- **Descriptive analytics** — vital-sign distributions, patient-wise and time-window summaries, abnormal-event frequency
- **Diagnostic analytics** — correlation analysis between physiological variables, temporal pattern analysis, identification of factors associated with abnormal measurements
- **Predictive analytics** — patient risk scoring, anomaly detection, short-term trend indicators using models such as Logistic Regression, Random Forest, Gradient Boosting, XGBoost, and Isolation Forest, evaluated using metrics such as Precision, Recall, F1-score, ROC-AUC, and PR-AUC under patient-wise (subject-level) cross-validation to prevent records from the same patient appearing in both training and test splits
- **Real-time analytics** — streaming aggregation over sliding/tumbling windows, continuous anomaly detection, and alert generation

SHAP-based explainability is incorporated to identify which physiological variables contributed most to a given risk or anomaly score, supporting interpretability in a healthcare context. To evaluate the benefit of the distributed approach, an experimental comparison between conventional single-machine processing and the proposed distributed pipeline will be conducted, measuring processing time, throughput, scalability, and, where applicable, streaming latency; these values will be obtained empirically during implementation and are not assumed in advance.

---

## 6. Technology / Tools Used

The technology stack is organized to reflect the flow from data sources through ingestion, storage, processing, analytics, machine learning, and visualization:

| Category                      | Tools / Technologies                                                                 |
|-------------------------------|--------------------------------------------------------------------------------------|
| **Big Data Processing**       | Hadoop, HDFS, MapReduce, Hive, Apache Spark, PySpark, Spark SQL, Spark Structured Streaming, Apache Kafka, HBase |
| **Data Processing**           | Python, Pandas, NumPy, PyArrow                                                       |
| **Machine Learning**          | Scikit-learn, XGBoost, Spark MLlib, imbalanced-learn, SHAP                           |
| **Visualization / Dashboard** | Plotly, Streamlit, Matplotlib                                                        |
| **Development Tools**         | Jupyter Notebook / Google Colab, Git, GitHub                                         |

---

## 7. Expected Outcome

The project is expected to result in a working, scalable Big Data pipeline for processing and analyzing multivariate patient vital-sign data, comprising:

- Distributed storage and processing of time-series healthcare data
- Batch analytics over historical data
- Streaming analytics over simulated real-time data
- Cleaned and preprocessed physiological datasets
- Statistical and temporal feature sets
- Descriptive, diagnostic, and predictive analytical outputs
- A trained and evaluated anomaly/risk-detection model with associated evaluation metrics
- SHAP-based explainable outputs
- An interactive visualization dashboard for patient trends and risk indicators
- A demonstrable real-time monitoring component
- An empirical comparison of single-machine versus distributed processing performance

Specific accuracy, latency, or scalability figures are not predetermined and will be reported as measured during implementation.

---

## 8. Potential Applications

As an academic prototype rather than a clinically certified medical device, the proposed system could serve as a foundation for exploring applications such as:

- Remote patient monitoring
- Supplementary hospital ward and ICU/critical-care monitoring support
- Telemedicine-oriented physiological data analysis
- Wearable-health analytics
- Longitudinal patient trend monitoring
- Healthcare research involving large-scale physiological datasets

It may also serve as a reference architecture for clinical decision-support research that requires scalable Big Data processing rather than diagnostic decision-making.

---

## 9. Future Scope

Future extensions of this project may include:

- Integration with real medical IoT devices and live hospital bedside-monitor streams using Kafka with Spark Structured Streaming for near-real-time risk updates
- Deployment on cloud-based infrastructure
- Incorporation of edge-computing components for on-device preprocessing
- Exploration of federated learning and other privacy-preserving analytics techniques (relevant given the sensitivity of patient data)
- Inclusion of additional physiological signals such as continuous ECG-derived arrhythmia indicators
- Development of personalized, patient-specific baselines rather than population-level thresholds
- Adoption of more advanced temporal and deep-learning models such as LSTM or Transformer-based sequence models
- Integration with hospital information systems
- Larger-scale real-time deployments across multiple wards or facilities
- Delivery of alerts through a mobile or SMS-based notification layer for nursing staff
- Subject to appropriate ethical and clinical governance, formal clinical validation of the system’s outputs

These extensions are proposed as future work and are not part of the current implementation.
