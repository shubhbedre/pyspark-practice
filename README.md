# Pyspark-practice

Pyspark practice problems which will cover most of the functionalities of spark.

# Spark Installation Steps
*Installing Spark 2 with PySpark on Windows 10 involves several steps, including downloading and installing Java, Spark, and setting up environment variables.* 

**Here's a step-by-step guide:**

## Step 1: Install Java Development Kit (JDK)

1. Download the JDK from the [Oracle website](https://www.oracle.com/java/technologies/javase-jdk8-downloads.html).
2. Install the JDK by following the on-screen instructions.
3. Set the `JAVA_HOME` environment variable:
   - Right-click on `This PC` -> `Properties` -> `Advanced system settings`.
   - Click on `Environment Variables`.
   - Under `System variables`, click `New` and set:
     - **Variable name**: `JAVA_HOME`
     - **Variable value**: `C:\Program Files\Java\jdk<version>` (replace `<version>` with your installed version)
   - Add `%JAVA_HOME%\bin` to the `Path` environment variable.

## Step 2: Install Apache Spark

1. Download Apache Spark 2.x from the [official Spark website](https://archive.apache.org/dist/spark/).
2. Extract the downloaded file to a directory (e.g., `C:\spark`).
3. Set the `SPARK_HOME` environment variable:
   - Follow the same steps as setting `JAVA_HOME`.
   - **Variable name**: `SPARK_HOME`
   - **Variable value**: `C:\spark`
   - Add `%SPARK_HOME%\bin` to the `Path` environment variable.

## Step 3: Install Hadoop (Winutils)

1. Download the pre-built `winutils.exe` from the [GitHub repository](https://github.com/steveloughran/winutils).
2. Create a directory `C:\hadoop\bin` and place `winutils.exe` in this directory.
3. Set the `HADOOP_HOME` environment variable:
   - **Variable name**: `HADOOP_HOME`
   - **Variable value**: `C:\hadoop`
   - Add `%HADOOP_HOME%\bin` to the `Path` environment variable.

## Step 4: Install Python and PySpark

1. Download and install `Python 3.7.x` from the [official Python website](https://www.python.org/downloads/release/python-377/).
2. Install setuptools==57.5.0 using pip:
   ```sh
   pip install setuptools==57.5.0
4. Install PySpark 2.4.8 using pip:
   ```sh
   pip install pyspark==2.4.8
## Step 5: Verify the Installation

### 1. Verify Java Installation:
- Open Command Prompt and run:
    ```sh
    java -version
- Ensure that the java version displayed matches the version you installed.

### 1. Verify Spark Installation:
- Open Command Prompt and run:
- ```sh 
    spark-shell
- Ensure that Spark starts without errors.

### 1. Verify PySpark Installation:
- Open Command Prompt and run:
    ```sh
    pyspark
- Ensure that PySpark starts without errors.

## Additional Configuration (if needed)
- You might need to set the `PYSPARK_PYTHON` environment variable if you have multiple versions of Python installed. To do this:

    - Open the Environment Variables window.
    - Click "New" under System variables and add:
        - Variable name: PYSPARK_PYTHON
        - Variable value: C:\Path\To\Your\Python\python.exe.

## Conclusion
*Following these steps will set up Spark 2 with PySpark on your Windows 10 machine. If you encounter any issues, ensure that all paths are set correctly and that you have the required permissions to modify environment variables.*
