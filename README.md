# Spark Interview Master

Enterprise-level Apache Spark learning and interview preparation repository covering **Scala, Java, and Spark SQL**.

This project is designed as a hands-on reference for senior-level Spark interviews and real-world Big Data engineering.

---

## 🎯 Project Goals

This repository covers Apache Spark from fundamentals to advanced enterprise concepts through practical examples.

The project focuses on:

* Spark Core
* DataFrames and Datasets
* Spark SQL
* Joins
* Aggregations
* Window Functions
* Partitioning
* Bucketing
* Data Skew
* Broadcast Joins
* Catalyst Optimizer
* Tungsten
* Adaptive Query Execution (AQE)
* Performance Optimization
* Caching and Persistence
* Serialization
* UDFs
* Structured Streaming
* Delta Lake
* Real-world data engineering scenarios
* Spark interview problems and solutions

---

## 🛠 Technology Stack

| Technology   | Version           |
| ------------ | ----------------- |
| Java         | 26.0.2.1          |
| Scala        | 2.12.18           |
| Apache Spark | 3.5.1             |
| Gradle       | 9.6.0             |
| Build Tool   | Gradle Kotlin DSL |
| Build System | Gradle Wrapper    |
| IDE          | IntelliJ IDEA     |

---

## 📚 Implementation Approach

Where applicable, concepts are implemented using three approaches:

### Scala

Spark's native Scala API.

### Java

Equivalent implementation using Spark's Java API.

### Spark SQL

Equivalent implementation using SQL.

This allows the same Spark concept to be understood from multiple perspectives and provides preparation for interviews where Scala, Java, or SQL may be expected.

---

## 📁 Project Structure

```text
spark-interview-master/
│
├── datasets/                    # Realistic learning datasets
│
├── src/
│   ├── main/
│   │   ├── java/                # Java implementations
│   │   ├── scala/               # Scala implementations
│   │   └── resources/           # Configuration and logging
│   │
│   └── test/
│       ├── java/                # Java tests
│       └── scala/               # Scala tests
│
├── build.gradle.kts              # Gradle build configuration
├── settings.gradle.kts           # Gradle project settings
├── gradle.properties              # Project versions/properties
│
├── gradlew                        # Gradle Wrapper - Unix/Git Bash
├── gradlew.bat                    # Gradle Wrapper - Windows
│
├── LICENSE
└── README.md
```

---

# 🚀 Modules

## Phase 1 — Project Foundation

### Module 1.1 — Repository Bootstrap ✅

Completed.

Topics covered:

* GitHub repository setup
* MIT License
* `.gitignore`
* Gradle Kotlin DSL
* Gradle Wrapper
* Java 26 configuration
* Scala 2.12 configuration
* Spark 3.5.1 dependencies
* Java source structure
* Scala source structure
* Test source structure
* Dataset directory
* Initial application
* Successful Gradle build
* Successful application execution

### Module 1.2 — Spark Foundation ⏳

Coming next:

* SparkSession
* DataFrame
* Dataset
* Spark SQL
* Transformations
* Actions
* Lazy evaluation
* Logical plans
* Physical plans
* `explain()`

### Module 1.3 — Enterprise SparkSession

Planned:

* SparkSession factory
* Environment-specific configuration
* Application configuration
* Local vs cluster configuration
* Spark configuration management

### Module 1.4 — Configuration

Planned:

* Typesafe Config
* Environment configuration
* Spark configuration
* Runtime parameters

### Module 1.5 — Logging

Planned:

* Log4j2
* Structured logging
* Application logging
* Spark logging configuration

### Module 1.6 — Dataset Repository

Planned:

* CSV
* JSON
* Parquet
* Realistic datasets
* Data generation utilities

### Module 1.7 — Coding Standards

Planned:

* Project conventions
* Naming standards
* Testing standards
* Documentation standards
* Performance guidelines

---

## 🔥 Planned Advanced Topics

The repository will progressively cover:

```text
DataFrames
    ↓
Spark SQL
    ↓
Joins
    ↓
Aggregations
    ↓
Window Functions
    ↓
Partitioning
    ↓
Data Skew
    ↓
Broadcast Joins
    ↓
Bucketing
    ↓
Caching
    ↓
Catalyst Optimizer
    ↓
Tungsten
    ↓
AQE
    ↓
Performance Tuning
    ↓
Structured Streaming
    ↓
Delta Lake
    ↓
Enterprise Interview Problems
```

---

## 🧪 Running the Project

### Build

Git Bash:

```bash
./gradlew clean build
```

Windows:

```powershell
.\gradlew.bat clean build
```

### Run

```bash
./gradlew run
```

Windows:

```powershell
.\gradlew.bat run
```

---

## 📌 Current Status

```text
Phase 1
├── Module 1.1  Repository Bootstrap       ✅
├── Module 1.2  Spark Foundation           ⏳
├── Module 1.3  Enterprise SparkSession    ⏳
├── Module 1.4  Configuration              ⏳
├── Module 1.5  Logging                    ⏳
├── Module 1.6  Dataset Repository         ⏳
└── Module 1.7  Coding Standards           ⏳
```

---

## 🎓 Interview Focus

Each major topic will eventually contain:

* Concept explanation
* Scala implementation
* Java implementation
* Spark SQL implementation
* Sample data
* Expected output
* Execution plan
* Performance considerations
* Common mistakes
* Best practices
* Interview questions
* Interview coding problems

The goal is not simply to learn Spark APIs, but to understand **how Spark executes queries and how to design performant production-grade Spark applications**.

---

## 👤 Author

**Shrikant Karvade**

Lead Big Data Engineer / Engineering Manager

Primary technologies:

* Java
* Scala
* Apache Spark
* Hadoop
* Hive
* Kafka
* Big Data Engineering
* Data Processing
* Performance Optimization
* AI-assisted Development
