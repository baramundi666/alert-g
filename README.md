# Alert-G - Alerts organization with Grafana MCP support

## Authors
- Mateusz Król
- Iga Antonik
- Łukasz Wilański
- Jakub Kotara

## Table of Contents
1. [Introduction](#introduction)
2. [Theoretical background/ technology stack](#theoretical-background)
3. [Case study concept description](#case-study-concept)
    1. [Application](#application)
    2. [Observability](#observability)
    3. [Vizualization](#vizualization)
4. [Case study high level architecture](#case-study-high-level-architecture)
5. [Case study detailed architecture](#case-study-detailed-architecture)
6. [Environment configuration description](#environment-configuration)
7. [Installation method](#installation)
8. [Demo deployment steps](#demo-deployment)
    1. [Configuration set-up](#configuration-setup)
    2. [Data preparation](#data-preparation)
9. [Demo description](#demo-description)
    1. [Execution procedure](#execution-procedure)
    2. [Results presentation](#results-presentation)
10. [Summary - conclusions](#summary-conclusions)
11. [References](#references)


## 1. Introduction <a name="introduction"></a>
Modern information systems, built on microservices architectures and orchestrated by the Kubernetes platform, generate an unprecedented volume of diagnostic data. In an era of dynamic scaling and Continuous Deployment (CI/CD), traditional monitoring approaches based on static dashboards are becoming insufficient. The primary challenge is no longer the collection of data itself, but rather its correlation and rapid interpretation during critical incidents.

There is a vital need to reduce the Mean Time To Recovery (MTTR), especially in distributed environments where a single component failure can trigger a domino effect across the entire cluster. To date, diagnostic tools have required administrators to be proficient in specialized query languages (such as PromQL for Prometheus) and to manually trace complex dependencies between services.

This project presents a modern approach to Observability, where the monitoring process is enhanced by Large Language Models (LLM). By leveraging the Model Context Protocol (MCP), we create an interface that allows AI models to interact directly with real-time operational data. The goal of this project is to demonstrate that integrating monitoring systems (Grafana Cloud) with intelligent agents allows for the automated analysis of root causes and dynamic environment configuration using natural language. This transition shifts the role of the system administrator from a manual dashboard analyst to a high-level AI operator.
## 2. Theoretical background/ technology stack <a name="theoretical-background"></a>
k8s -  mini kube/ AWS, opentelemetry/ prometheus, MCP, graphana (graphana cloud/ graphana hosted locally), graphana observability, "graphana assistant" (?), LLM

## 3. Case study concept description <a name="case-study-concept"></a>
- e.g. killing a pod in order to verify graphana's observability capabilities

e.g.:
- we see an alert
- we want more information about it
- we inquire MCP server for additional data about inter services communication
### 3.1 Application <a name="application"></a>
### 3.2 Observability <a name="observability"></a>
### 3.3 Vizualization <a name="vizualization"></a>

## 4. Case study high level architecture <a name="case-study-high-level-architecture"></a>
demo architecture - application, observability, actors

## 5. Case study detailed architecture <a name="case-study-detailed-architecture"></a>

## 6. Environment configuration description <a name="environment-configuration"></a>

## 7. Installation method <a name="installation"></a>

## 8. Demo deployment steps <a name="demo-deployment"></a>
### 8.1 Configuration set-up <a name="configuration-setup"></a>
### 8.2 Data preparation <a name="data-preparation"></a>

## 9. Demo description <a name="demo-description"></a>
### 9.1 Execution procedure <a name="execution-procedure"></a>
### 9.2 Results presentation – All prompts used with AI models should be listed, screens from Grafana dashboard should be attached <a name="results-presentation"></a>

## 10. Summary – conclusions <a name="summary-conclusions"></a>

## 11. References <a name="references"></a>
- https://grafana.com/docs/grafana/latest/alerting/
- https://community.grafana.com/t/how-to-setup-the-grafana-mcp-server-complete-guide/155923

