# Module 1.3 — Enterprise SparkSession Architecture

## Objective

A Spark application should not create and configure `SparkSession` in an ad-hoc manner.

In an enterprise application, configuration, environment selection, session lifecycle, and Spark execution should have clear ownership.

The objective of this module is to establish a predictable architecture:

```text
Environment
     │
     ▼
Application Configuration
     │
     ▼
SparkApplicationConfig
     │
     ▼
EnterpriseSparkSession
     │
     ▼
SparkSession
     │
     ▼
SparkContext
```

The application should express **what it wants to execute**, while infrastructure code owns **how Spark is configured and managed**.

---

# 1. SparkSession Lifecycle

## 1.1 Creating a SparkSession

The application obtains its SparkSession through the enterprise factory:

```scala
val spark =
  EnterpriseSparkSession.create(
    appName = "My Spark Application",
    environment = environment
  )
```

Application code does not directly construct the environment-specific Spark configuration.

This creates a clear separation between:

* business/application logic
* infrastructure configuration
* Spark runtime management

---

## 1.2 `getOrCreate()`

The factory uses:

```scala
builder.getOrCreate()
```

`getOrCreate()` can reuse an existing SparkSession instead of blindly creating another session.

An experiment created two sessions using different application names and masters.

The observed result was:

```text
Same SparkSession instance : true
Same SparkContext instance : true

Session 1 app name : Module 1.3 - Enterprise SparkSession
Session 2 app name : Module 1.3 - Enterprise SparkSession

Session 1 master   : local[2]
Session 2 master   : local[2]
```

The second call did not replace the existing application identity or master.

### Engineering implication

`getOrCreate()` is not merely syntactic convenience.

It participates in SparkSession lifecycle and reuse semantics.

Therefore, enterprise applications should have a clearly defined owner for SparkSession creation rather than allowing arbitrary components to create sessions independently.

---

# 2. SparkSession and SparkContext

A SparkSession provides the higher-level SQL/DataFrame API while the underlying SparkContext manages the core Spark application runtime.

Conceptually:

```text
SparkSession
     │
     ▼
SparkContext
     │
     ├── Scheduler
     ├── Executors
     ├── BlockManager
     ├── Spark UI
     └── Other application services
```

Multiple SparkSessions can share the same SparkContext.

This was demonstrated using:

```scala
val session1 = spark
val session2 = spark.newSession()
```

The experiment showed:

```text
Same SparkContext : true
```

while the sessions maintained independent SQL configuration.

---

# 3. `newSession()` and Configuration Isolation

`newSession()` creates a separate SparkSession while sharing the existing SparkContext.

The experiment started with:

```text
Session 1 before override : 400
Session 2 before override : 400
```

Then Session 1 was modified:

```scala
session1.conf.set(
  "spark.sql.shuffle.partitions",
  10
)
```

The resulting configuration was:

```text
Session 1 after override  : 10
Session 2 after override  : 400
```

This demonstrates that SQL runtime configuration can be isolated at the SparkSession level.

Conceptually:

```text
                 SparkContext
                      │
             ┌────────┴────────┐
             │                 │
        SparkSession 1    SparkSession 2
             │                 │
       shuffle = 10       shuffle = 400
```

### Important distinction

Not every Spark property belongs to the same configuration scope.

A useful architectural distinction is:

| Configuration scope      | Example                                   | Ownership                          |
| ------------------------ | ----------------------------------------- | ---------------------------------- |
| Application/SparkContext | master, application identity              | Application infrastructure         |
| SparkSession/SQL         | `spark.sql.shuffle.partitions`            | Session/application                |
| Runtime SQL              | temporary tuning through `spark.conf.set` | Application logic, where justified |
| Environment              | dev/test/prod settings                    | Deployment/configuration layer     |

This prevents configuration ownership from becoming ambiguous.

---

# 4. Session Lifecycle and `stop()`

A lifecycle experiment created a third session:

```scala
val session3 =
  spark.newSession()
```

The experiment confirmed:

```text
Shared SparkContext : true
```

The secondary session was then stopped:

```scala
session3.stop()
```

Observed behavior:

```text
SparkContext is stopping
Successfully stopped SparkContext
```

A subsequent operation through the original session failed:

```text
Session 1 unusable after session3.stop :
Cannot call methods on a stopped SparkContext.
```

### Engineering conclusion

In this Spark 3.5.1 environment, stopping the secondary session stopped the shared SparkContext.

Therefore, code should not assume that independently stopping a session created through `newSession()` is harmless.

### Lifecycle ownership principle

The application should have one clear owner responsible for Spark lifecycle.

A recommended application lifecycle is:

```text
Application Start
      │
      ▼
Resolve Environment
      │
      ▼
Load Configuration
      │
      ▼
Create SparkSession
      │
      ▼
Execute Application
      │
      ▼
Stop SparkSession
      │
      ▼
Application End
```

Sessions created for isolated SQL state should not be treated as independently owned Spark applications.

---

# 5. Configuration Hierarchy

Module experiments investigated multiple configuration layers.

## 5.1 SparkConf

Example:

```scala
val sparkConf =
  new SparkConf()
    .set("spark.sql.shuffle.partitions", "37")
```

The SparkConf object retained:

```text
37
```

---

## 5.2 SparkSession Builder

The builder subsequently specified:

```scala
.config(
  "spark.sql.shuffle.partitions",
  29
)
```

The effective SparkSession configuration became:

```text
29
```

---

## 5.3 Runtime SQL Configuration

The application then executed:

```scala
spark.conf.set(
  "spark.sql.shuffle.partitions",
  11
)
```

The effective value became:

```text
11
```

The experiment therefore demonstrated:

```text
SparkConf
    37
     │
     ▼
SparkSession Builder
    29
     │
     ▼
Runtime SQL configuration
    11
```

### Important qualification

This experiment demonstrates the behavior for this runtime SQL configuration in this setup.

It should not be generalized into a universal precedence rule for every Spark property.

Spark configuration behavior depends on the property and the stage at which it is applied.

---

# 6. Environment-Specific Configuration

Initially, environment selection only changed the application name:

```text
dev  → Module 1.3 - Environment - dev
test → Module 1.3 - Environment - test
prod → Module 1.3 - Environment - prod
```

However, Spark configuration remained identical.

This exposed an architectural weakness:

> Environment selection is useful only when it selects the corresponding configuration set.

The design was therefore changed to:

```text
Environment
     │
     ▼
application-{environment}.conf
     │
     ▼
Typed Configuration
     │
     ▼
SparkSession Factory
```

---

# 7. Configuration-Driven Architecture

Environment-specific configuration is stored externally.

Example:

```text
src/main/resources/config/application-dev.conf
src/main/resources/config/application-test.conf
src/main/resources/config/application-prod.conf
```

Example development configuration:

```hocon
environment = "dev"

spark {
  master = "local[2]"
  sql.shuffle.partitions = 20
  sql.adaptive.enabled = true
}
```

Test configuration uses a different shuffle configuration:

```text
50
```

Production configuration uses:

```text
400
```

The application therefore does not contain:

```scala
if (environment == "dev") {
  ...
} else if (environment == "test") {
  ...
} else if (environment == "prod") {
  ...
}
```

Instead:

```text
Environment
     │
     ▼
Configuration File
     │
     ▼
Typed Configuration
```

This keeps environment concerns outside application logic.

---

# 8. Typed Configuration

Raw configuration access was separated from SparkSession creation through:

```scala
final case class SparkApplicationConfig(
  environment: String,
  master: String,
  shufflePartitions: Int,
  adaptiveExecutionEnabled: Boolean
)
```

The architecture becomes:

```text
HOCON
  │
  ▼
SparkApplicationConfig
  │
  ▼
EnterpriseSparkSession
```

This provides several advantages:

* configuration becomes type-safe
* validation can happen before Spark startup
* configuration loading can be unit tested independently
* SparkSession creation does not need to understand HOCON
* application code consumes a domain object instead of raw configuration

---

# 9. Configuration Validation

The configuration layer validates required values before SparkSession creation.

For example:

```scala
require(
  config.master.nonEmpty,
  "spark.master must not be empty"
)
```

and:

```scala
require(
  config.shufflePartitions > 0,
  "spark.sql.shuffle.partitions must be greater than 0"
)
```

Unsupported environments are rejected before Spark startup:

```text
Unsupported environment: 'uat'.
Supported environments: dev, test, prod
```

This is important operationally.

A configuration error should fail during application startup rather than after Spark has already allocated runtime resources.

---

# 10. Domain-Level Configuration Exceptions

Low-level HOCON configuration exceptions are translated into an application-specific exception:

```scala
final class SparkApplicationConfigException(
  message: String,
  cause: Throwable = null
) extends RuntimeException(message, cause)
```

The original configuration exception is preserved as the cause.

Therefore, the error boundary becomes:

```text
HOCON ConfigException
        │
        ▼
SparkApplicationConfigException
        │
        ▼
Application
```

This prevents configuration-library details from leaking through the application's infrastructure boundary.

---

# 11. Testing Strategy

Configuration resolution is deliberately separated from SparkSession creation.

The core function:

```scala
fromConfig(
  environment: String,
  config: Config
)
```

can therefore be tested without starting Spark.

Tests cover:

* supported environments
* unsupported environments
* valid configuration
* missing configuration properties
* empty Spark master
* invalid shuffle partition values
* configuration exception translation
* preservation of the original cause

This creates a faster and more deterministic unit-test boundary.

The project currently has:

```text
13+ configuration/session tests
```

with successful Gradle test execution.

---

# 12. Runtime Configuration Ownership

The enterprise factory establishes the initial configuration:

```text
Environment
     │
     ▼
SparkApplicationConfig
     │
     ▼
EnterpriseSparkSession
     │
     ▼
SparkSession
```

However, Spark SQL configuration remains mutable.

For example:

```scala
spark.conf.set(
  "spark.sql.shuffle.partitions",
  10
)
```

The session isolation experiment demonstrated that such a change affects one SparkSession without changing another session's SQL configuration.

Therefore, the architecture should not attempt to make every runtime configuration immutable.

Instead, configuration should have explicit ownership.

### Recommended principle

**Centralize defaults; control runtime overrides.**

Infrastructure configuration should establish predictable defaults.

Application code may make runtime SQL changes when there is a legitimate execution reason, but those changes should be deliberate and visible.

---

# 13. Enterprise Architecture

The resulting design is:

```text
                    Environment
                         │
                         ▼
              Application Configuration
                         │
                         ▼
              SparkApplicationConfig
                         │
                         ▼
             EnterpriseSparkSession
                         │
                         ▼
                   SparkSession
                         │
                         ▼
                   SparkContext
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
          Scheduler   Executors   Spark UI
```

The application itself remains focused on business processing:

```text
Application
     │
     ├── Resolve environment
     │
     ├── Obtain SparkSession
     │
     ├── Execute business logic
     │
     └── Complete application
```

Spark infrastructure remains centralized:

```text
EnterpriseSparkSession
     │
     ├── Configuration loading
     ├── Environment handling
     ├── Spark configuration
     └── Session lifecycle
```

---

# 14. Key Engineering Lessons

## Lesson 1 — SparkSession is an application infrastructure concern

Do not allow every component to create its own SparkSession.

---

## Lesson 2 — SparkSession and SparkContext have different scopes

Multiple sessions can share a SparkContext while maintaining independent SQL/session state.

---

## Lesson 3 — `getOrCreate()` has lifecycle implications

It may reuse an existing session/context rather than creating a completely independent Spark application.

---

## Lesson 4 — Environment must select configuration

Simply changing an application name based on environment is not environment-aware configuration.

---

## Lesson 5 — Configuration should be typed

A domain configuration object provides a cleaner boundary than passing raw configuration objects throughout the application.

---

## Lesson 6 — Validate before Spark startup

Invalid configuration should fail fast before expensive Spark runtime initialization.

---

## Lesson 7 — Runtime SQL configuration is mutable

Centralized configuration does not mean every Spark setting must be immutable.

The important architectural question is:

> Who owns the default, and who is allowed to override it?

---

## Lesson 8 — Lifecycle ownership must be explicit

When multiple sessions share a SparkContext, stopping one session can affect the entire shared runtime.

The application should therefore have a clear lifecycle owner.

---

# 15. Final Design Principle

The goal is not to build a complicated Spark framework.

The goal is to establish a small, explicit infrastructure boundary:

```text
Application
     │
     │ business intent
     ▼
EnterpriseSparkSession
     │
     │ infrastructure configuration
     ▼
SparkSession
     │
     ▼
SparkContext
```

This keeps Spark configuration predictable, environment-aware, testable, and maintainable while allowing application code to remain focused on data processing rather than infrastructure management.
