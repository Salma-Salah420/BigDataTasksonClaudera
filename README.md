# BigDataTasksonClaudera
----------------------------
 Big Data Project – My Tasks
 

This repository contains my implementation of key Big Data concepts using distributed processing techniques.

 Overview
 ---------------

This project demonstrates practical applications of Big Data processing including:

Data partitioning
Distributed caching
Reduce-side join operations


🧩 Tasks
--------------
🔹 Task 1: Partitioner:


Implemented a custom partitioner to control how data is distributed across reducers.

Key Concepts:

Custom partitioning logic
Load balancing across reducers
Improved performance for distributed processing

Goal:
Ensure that related data is sent to the same reducer to optimize processing efficiency.
-----------------------------------------------------------

🔹 Task 2: Distributed Cache

Used Distributed Cache to share small read-only files across all nodes.

Key Concepts:

Efficient data sharing in distributed systems
Reducing network overhead
Improving performance by avoiding repeated data reads

Use Case:
Loading lookup/reference data efficiently in mapper or reducer.
---------------------------------------------

🔹 Task 3: Reduce-Side Join

Implemented a Reduce-side join to combine datasets based on a common key.

Key Concepts:

Joining large datasets in distributed systems
Handling multiple input sources
Key-based grouping

Process:

Mapper tags records from different datasets
Shuffle phase groups by key
Reducer performs the join operation

--------------------------------
🛠️ Technologies Used
-----------------------
Hadoop MapReduce
Java / Python (depending on your implementation)
HDFS (Hadoop Distributed File System)
--------------------------
📌 How to Run
Upload input data to HDFS
Compile the code
Run each task using Hadoop commands:
hadoop jar your-jar-file.jar MainClass input output.
----------------------------------
📈 Learning Outcomes
Understanding distributed data processing
Optimizing performance using partitioning and caching
Implementing joins in Big Data environments
---------------------

Salma Salah
