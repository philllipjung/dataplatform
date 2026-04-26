# Jenkins CI/CD Pipeline for Hynix

## Table of Contents
1. [What is Jenkins?](#what-is-jenkins)
2. [Pipeline Concepts](#pipeline-concepts)
3. [Groovy for Jenkins](#groovy-for-jenkins)
4. [Project Pipeline Structure](#project-pipeline-structure)
5. [Setup Guide](#setup-guide)
6. [Running the Pipeline](#running-the-pipeline)
7. [Troubleshooting](#troubleshooting)

---

## What is Jenkins?

Jenkins is an open-source automation server that helps automate the software development process:

- **Continuous Integration (CI)**: Automatically builds and tests code changes
- **Continuous Deployment (CD)**: Automatically deploys code to environments
- **Pipeline**: Defines the steps Jenkins should execute as code

**Think of Jenkins as:**
- A robot that runs commands for you automatically
- A scheduler that triggers jobs when events happen (code push, manual trigger, etc.)
- A coordinator that connects different tools (Git, build tools, deployment tools)

---

## Pipeline Concepts

### What is a Jenkins Pipeline?

A **Pipeline** is a series of steps (stages) that Jenkins executes to complete a task.

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Checkout   │───▶│    Update   │───▶│   Commit    │───▶│    Push    │
│   Stage     │    │   Stage     │    │   Stage     │    │   Stage     │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

### Pipeline Syntax Types

**1. Declarative Pipeline (Recommended)**
```groovy
pipeline {
    agent any
    stages {
        stage('Build') {
            steps {
                sh 'make build'
            }
        }
    }
}
```

**2. Scripted Pipeline (Legacy)**
```groovy
node {
    stage('Build') {
        sh 'make build'
    }
}
```

**This project uses Declarative Pipeline.**

---

## Groovy for Jenkins

### What is Groovy?

Groovy is a programming language for the Java platform. Jenkins uses Groovy because:
- It runs on the Java Virtual Machine (JVM)
- It has flexible syntax (easier than Java)
- It can integrate with Java libraries

### Basic Groovy Concepts Used in Jenkins

#### 1. Variables and Strings
```groovy
// Single quotes = literal string
def name = 'world'

// Double quotes = string interpolation
def greeting = "Hello, ${name}"  // Result: "Hello, world"

// Triple quotes = multiline string
def multiline = """
    Line 1
    Line 2
"""
```

#### 2. Lists and Maps
```groovy
// List (like array)
def items = ['apple', 'banana', 'orange']
println items[0]  // Output: apple

// Map (like dictionary/hash)
def person = [name: 'John', age: 30]
println person.name  // Output: John
```

#### 3. Closures (Code Blocks)
```groovy
// Closure is a block of code you can pass around
def greet = { name ->
    println "Hello, ${name}"
}

greet('Alice')  // Output: Hello, Alice
```

#### 4. File Operations
```groovy
// Read file
def content = readFile file: 'config.json'

// Write file
writeFile file: 'output.txt', text: 'Hello World'

// Execute shell command
sh 'ls -la'
```

---

## Project Pipeline Structure

### Pipeline Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Jenkins Pipeline                            │
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │  Checkout    │  │   Update     │  │   Commit     │   │
│  │  Repository  │  │  Build No.   │  │  Changes     │   │
│  └──────────────┘  └──────────────┘  └──────────────┘   │
│         │                  │                 │               │
│         └──────────────────┴─────────────────┴──────────────┘
│                              │
│                              ▼
│                       ┌──────────────┐
│                       │    Push     │
│                       │  to GitHub  │
│                       └──────────────┘
└─────────────────────────────────────────────────────────────────┘
```

### Jenkinsfile Breakdown

Let's examine the actual `Jenkinsfile` line by line:

```groovy
pipeline {
    agent any
```
- **`pipeline`**: Defines a declarative pipeline
- **`agent any`**: Run this pipeline on any available Jenkins agent (server)

```groovy
    environment {
        PROVISION_ID = '0002_wfbm'
        GITHUB_REPO = 'https://github.com/philllipjung/hynix.git'
        CONFIG_FILE = 'config/config.json'
        GIT_CREDENTIALS_ID = 'github-creds'
    }
```
- **`environment`**: Global variables available in all stages
- **Usage**: `${PROVISION_ID}` - Use `${}` to reference variables

#### Stage 1: Checkout

```groovy
    stage('Checkout') {
        steps {
            echo "Checking out repository..."
            git branch: 'master',
                url: "${GITHUB_REPO}",
                credentialsId: "${GIT_CREDENTIALS_ID}"
        }
    }
```

**Breakdown:**
- **`stage('Checkout')`**: Names this stage "Checkout"
- **`steps { ... }`**: Defines actions to take
- **`echo "..."`**: Print message to Jenkins console
- **`git branch: 'master', url: "..."`**: Clone Git repository
  - `branch`: Which branch to checkout
  - `url`: Repository URL
  - `credentialsId`: ID of stored credentials in Jenkins

#### Stage 2: Update Build Number

```groovy
    stage('Update Build Number') {
        steps {
            script {
                echo "Updating build number for provision_id: ${PROVISION_ID}"
                echo "Current BUILD_NUMBER: ${BUILD_NUMBER}"

                // Read config.json
                def configFile = readFile file: "${CONFIG_FILE}"
                def config = new groovy.json.JsonSlurper().parseText(configFile)
```

**Breakdown:**
- **`script { ... }`**: Allows writing Groovy code directly
- **`def configFile = readFile file: "..."`**: Read file content into variable
- **`JsonSlurper()`**: JSON parser in Groovy
- **`parseText(configFile)`**: Parse JSON string into Groovy objects

```groovy
                // Find the target provision entry
                def targetEntry = config.config_specs.find { it.provision_id == "${PROVISION_ID}" }
```
- **`find { ... }`**: Search list for matching item
- **`it.provision_id`**: Access property of current item
- **Returns**: First matching item or `null`

```groovy
                if (targetEntry) {
                    def currentNumber = targetEntry.build_number.number
                    echo "Current minor version in config: ${currentNumber}"

                    // Update minor version to BUILD_NUMBER
                    targetEntry.build_number.number = "${BUILD_NUMBER}"
                    echo "Updated minor version: ${currentNumber} -> ${BUILD_NUMBER}"
```
- **`if (targetEntry)`**: Check if item was found (not null)
- **`${BUILD_NUMBER}`**: Jenkins built-in variable = current build number

```groovy
                    // Write back to config.json
                    writeFile file: "${CONFIG_FILE}", text: groovy.json.JsonOutput.toJson(config)
                    // Pretty print JSON
                    sh "jq '.' ${CONFIG_FILE} > ${CONFIG_FILE}.tmp && mv ${CONFIG_FILE}.tmp ${CONFIG_FILE}"
```
- **`writeFile`**: Write content to file
- **`JsonOutput.toJson(config)`**: Convert Groovy object back to JSON string
- **`sh "..."`**: Execute shell command
  - Uses `jq` to format JSON nicely

#### Stage 3: Commit Changes

```groovy
    stage('Commit Changes') {
        steps {
            script {
                sh """
                    git config user.email "jenkins@hynix.local"
                    git config user.name "Jenkins Agent"
                    git add ${CONFIG_FILE}
                    git commit -m "Update build_number to \${BUILD_NUMBER}"
                """
            }
        }
    }
```
- **Triple quotes `"""`**: Multiline shell script
- **`\${BUILD_NUMBER}`**: Escaped `$` so Groovy doesn't replace it (shell will)
- **Commands**:
  1. Configure Git user
  2. Stage changed file
  3. Commit with message

#### Stage 4: Push Changes

```groovy
    stage('Push Changes') {
        steps {
            script {
                withCredentials([usernamePassword(
                    credentialsId: "${GIT_CREDENTIALS_ID}",
                    usernameVariable: 'GIT_USERNAME',
                    passwordVariable: 'GIT_PASSWORD'
                )]) {
                    sh """
                        git push https://\${GIT_USERNAME}:\${GIT_PASSWORD}@github.com/philllipjung/hynix.git master
                    """
                }
            }
        }
    }
```
- **`withCredentials([ ... ])`**: Jenkins security feature
  - Retrieves credentials from Jenkins credentials store
  - Makes them available as environment variables
  - Masks them in logs (passwords hidden)
- **`${GIT_USERNAME}`**: Variable name from `usernameVariable`
- **`${GIT_PASSWORD}`**: Variable name from `passwordVariable`

#### Post Actions

```groovy
    post {
        success {
            echo "Pipeline completed successfully - minor version updated to ${BUILD_NUMBER}"
        }
        failure {
            echo "Pipeline failed!"
        }
    }
```
- **`post`**: Actions after stages complete
- **`success`**: Run if all stages succeeded
- **`failure`**: Run if any stage failed

---

## Setup Guide

### 1. Jenkins Installation

```bash
# Run Jenkins in Docker
docker run -d \
  --name jenkins \
  --restart unless-stopped \
  -p 8085:8080 \
  -p 50000:50000 \
  -v jenkins_home:/var/jenkins_home \
  jenkins/jenkins:2.426.3-jdk11

# Get initial admin password
docker exec jenkins cat /var/jenkins_home/secrets/initialAdminPassword
```

### 2. Access Jenkins

1. Open browser: http://localhost:8085
2. Unlock Jenkins with initial password
3. Install suggested plugins
4. Create admin user

### 3. Configure Credentials

**Add GitHub Credentials:**
1. Go to: **Manage Jenkins** → **Manage Credentials**
2. Click: **(global)** → **Add Credentials**
3. Fill in:
   - **Kind**: Username with password
   - **Username**: Your GitHub username
   - **Password**: GitHub Personal Access Token (PAT)
   - **ID**: `github-creds` (important!)
4. Click **Create**

**Create GitHub PAT:**
1. GitHub → Settings → Developer settings → Personal access tokens → Tokens (classic)
2. Generate new token (classic)
3. Scopes: `repo` (full control)
4. Copy token (you won't see it again!)

### 4. Create Jenkins Job

**Option 1: Using Web UI**
1. Click **New Item**
2. Name: `pilot-pipeline`
3. Type: **Pipeline**
4. Configure:
   - **Definition**: Pipeline script from SCM
   - **SCM**: Git
   - **Repository URL**: https://github.com/philllipjung/hynix.git
   - **Credentials**: github-creds
   - **Branch**: master
   - **Script Path**: Jenkinsfile
5. Click **Save**

**Option 2: Using Script**
```bash
# Use Jenkins CLI
docker exec jenkins java -jar /tmp/jenkins-cli.jar \
  -s http://localhost:8080 \
  -auth admin:<password> \
  create-job pilot-pipeline < job_config.xml
```

---

## Running the Pipeline

### Method 1: Web UI

1. Open: http://localhost:8085/job/pilot-pipeline/
2. Click **"Build Now"**
3. Click build number (e.g., #1)
4. Click **"Console Output"** to see logs

### Method 2: CLI

```bash
# Trigger build
curl -X POST http://localhost:8085/job/pilot-pipeline/build \
  --user "admin:<password>"

# Check build status
curl --user "admin:<password>" \
  http://localhost:8085/job/pilot-pipeline/lastBuild/api/json
```

### Method 3: API with Parameters

```bash
# Trigger with custom PROVISION_ID
curl -X POST "http://localhost:8085/job/pilot-pipeline/buildWithParameters?PROVISION_ID=0004_wfbm" \
  --user "admin:<password>"
```

---

## Understanding the Build Process

### What Happens During a Build?

```
┌──────────────────────────────────────────────────────────────┐
│ 1. CHECKOUT STAGE                                      │
│    ├─ Jenkins clones the GitHub repository                 │
│    ├─ Switches to master branch                         │
│    └─ Workspace: /var/jenkins_home/workspace/pilot-pipeline│
├──────────────────────────────────────────────────────────────┤
│ 2. UPDATE BUILD NUMBER STAGE                            │
│    ├─ Read config/config.json                           │
│    ├─ Parse JSON with Groovy                            │
│    ├─ Find entry with provision_id = "0002_wfbm"       │
│    ├─ Update build_number.number = BUILD_NUMBER           │
│    └─ Write back to config.json (with jq formatting)     │
├──────────────────────────────────────────────────────────────┤
│ 3. COMMIT STAGE                                        │
│    ├─ Configure Git (user.email, user.name)             │
│    ├─ git add config/config.json                        │
│    └─ git commit -m "Update build_number to X"          │
├──────────────────────────────────────────────────────────────┤
│ 4. PUSH STAGE                                          │
│    ├─ Load credentials from Jenkins store                 │
│    ├─ git push to GitHub master branch                  │
│    └─ Credentials masked in logs                        │
├──────────────────────────────────────────────────────────────┤
│ 5. POST ACTIONS                                         │
│    ├─ If success: Print success message                   │
│    └─ If failure: Print error message                    │
└──────────────────────────────────────────────────────────────┘
```

### Example Console Output

```
[Pipeline] Starting
[Checkout] Checking out repository...
...
[Update Build Number] Updating build number for provision_id: 0002_wfbm
[Update Build Number] Current BUILD_NUMBER: 12
[Update Build Number] Current minor version in config: 11
[Update Build Number] Updated minor version: 11 -> 12
[Update Build Number] Build number updated successfully to: 12
...
[Commit Changes] + git config user.email "jenkins@hynix.local"
[Commit Changes] + git config user.name "Jenkins Agent"
[Commit Changes] + git add config/config.json
[Commit Changes] + git commit -m "Update build_number to 12"
...
[Push Changes] + git push https://****@github.com/philllipjung/hynix.git master
[Push Changes] To https://github.com/philllipjung/hynix.git
[Push Changes]    1a2b3c4..5d6e7f8  master -> master
...
[Pipeline] completed successfully - minor version updated to 12
```

---

## Versioning System

### Semantic Versioning

This project uses semantic versioning: **major.minor.patch**

| Component | Value | Location | Example |
|-----------|---------|----------|---------|
| Major | `4` (constant) | Hardcoded in API | `4` |
| Minor | BUILD_NUMBER | config.json | `12` |
| Patch | `1` (constant) | Hardcoded in API | `1` |

### Flow Diagram

```
┌──────────────────┐
│   Jenkins       │
│   Build #12     │
└────────┬─────────┘
         │
         ▼
┌─────────────────────────────────┐
│   config/config.json          │
│   {                         │
│     "build_number": {         │
│       "number": "12"  ◄──── Minor only (stored in Git)
│     }                       │
│   }                         │
└────────┬────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│   API Server                 │
│   /api/v1/spark/reference    │
│                             │
│   Constructs: 4.12.1        │ ◄─ Full version (major.minor.patch)
└─────────────────────────────────┘
```

### Why This Design?

1. **Jenkins stores only minor**: Simple number to increment
2. **API constructs full version**: Centralized version logic
3. **Major/Patch are constants**: Easy to change later in one place
4. **Git contains minimal data**: Cleaner history

---

## Troubleshooting

### Common Issues

#### 1. Build Failed - "Credentials not found"

**Error:**
```
ERROR: Credentials not found for github-creds
```

**Solution:**
1. Go to Manage Jenkins → Manage Credentials
2. Check if `github-creds` exists
3. If not, create it with your GitHub token

#### 2. Git Push Failed - "Authentication Error"

**Error:**
```
fatal: Authentication failed for 'https://github.com/...'
```

**Solution:**
1. Verify GitHub token has `repo` scope
2. Check token hasn't expired
3. Test credentials:
```bash
curl -u <username>:<token> https://api.github.com/user
```

#### 3. Config File Not Found

**Error:**
```
ERROR: File not found: config/config.json
```

**Solution:**
1. Check repository structure
2. Verify Jenkinsfile has correct `CONFIG_FILE` path
3. Ensure workspace directory exists

#### 4. jq Command Not Found

**Error:**
```
jq: command not found
```

**Solution:**
```bash
# Install jq in Jenkins container
docker exec jenkins apt-get update
docker exec jenkins apt-get install -y jq
```

Or modify Jenkinsfile to not use `jq`:
```groovy
writeFile file: "${CONFIG_FILE}",
    text: groovy.json.JsonOutput.toJson(config, true)  // true = pretty print
```

#### 5. Pipeline Script Approval Required

**Error:**
```
Script not yet approved for use
```

**Solution:**
1. Go to Manage Jenkins → **In-process Script Approval**
2. Find the pending script
3. Click **Approve**

---

## Advanced Topics

### Adding New Parameters

```groovy
parameters {
    string(name: 'PROVISION_ID',
           defaultValue: '0002_wfbm',
           description: 'Provision ID to update')
    string(name: 'DRY_RUN',
           defaultValue: 'false',
           description: 'Dry run (no commit)')
}

stages {
    stage('Update') {
        when {
            expression { params.DRY_RUN == 'false' }
        }
        steps {
            // ... update logic
        }
    }
}
```

### Conditional Execution

```groovy
stage('Update Build Number') {
    when {
        branch 'master'  // Only run on master branch
    }
    steps {
        // ...
    }
}
```

### Parallel Stages

```groovy
stages {
    stage('Parallel Tests') {
        parallel {
            stage('Unit Tests') {
                steps { sh 'make test-unit' }
            }
            stage('Integration Tests') {
                steps { sh 'make test-integration' }
            }
        }
    }
}
```

---

## Quick Reference

### Jenkins Built-in Variables

| Variable | Description | Example |
|-----------|-------------|----------|
| `BUILD_NUMBER` | Current build number | `12` |
| `BUILD_ID` | Same as BUILD_NUMBER | `12` |
| `JOB_NAME` | Name of the job | `pilot-pipeline` |
| `WORKSPACE` | Workspace path | `/var/jenkins_home/workspace/pilot-pipeline` |
| `NODE_NAME` | Agent name | `master` |

### Groovy Quick Reference

```groovy
// Variables
def x = 10
def y = "Hello"

// String interpolation
def result = "Value is ${x}"  // "Value is 10"

// Lists
def items = ['a', 'b', 'c']
items.each { println it }

// Maps
def map = [key: 'value']
println map.key

// Conditions
if (x > 5) {
    println "Greater than 5"
}

// Loops
10.times { i ->
    println "Count: ${i}"
}

// File operations
def content = readFile file: 'file.txt'
writeFile file: 'out.txt', text: content
sh 'ls -la'
```

---

## Further Reading

- [Jenkins Official Documentation](https://www.jenkins.io/doc/)
- [Pipeline Syntax Reference](https://www.jenkins.io/doc/book/pipeline/syntax/)
- [Groovy Documentation](http://groovy-lang.org/documentation.html)
- [Jenkins GitHub](https://github.com/jenkinsci/jenkins)

---

## Summary

### What You Learned

1. **Jenkins Basics**: What Jenkins is and why we use it
2. **Pipeline Structure**: Stages, steps, and post actions
3. **Groovy Fundamentals**: Variables, strings, file operations
4. **Project Pipeline**: How our specific Jenkinsfile works
5. **Setup & Execution**: How to create and run jobs
6. **Troubleshooting**: Common issues and solutions

### The Pipeline in One Sentence

> Jenkins clones our repository, updates the build number in config.json, commits the change, and pushes it back to GitHub.

---

**Last Updated**: 2026-02-12
**Jenkins Version**: 2.426.3
**Pipeline Type**: Declarative
