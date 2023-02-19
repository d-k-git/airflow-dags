#### Repository contains different examples of dags ####

### 1. bash_operator.py ###

**Description**: Runs shell commands in the HDFS cli

### 2. bash_sensor.py ###

**Description**:  Checks  if the file _SUCCESS is updated to the current date. If the condition is true, it means that new partitions have been loaded and can be processed

### 3. kubernetes_pod_operator.py ###

**Description**:  Enables task-level resource configuration and allows to create custom Python dependencies

### 4. hive_name_patririon_sensor.py ###

**Description**: Checks the update of partitions in Hive (HDFS) on the specified date

### 5. kafka_consumer_dag.py ###

**Description**:  Sets configurations and dependencies for running the consumer
