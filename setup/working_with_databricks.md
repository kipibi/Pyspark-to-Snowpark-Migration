# Working with Databricks

## Data Loading To DBFS (Databricks File System) 
The Databricks File System (DBFS) is a distributed file system mounted into a Databricks workspace and available on Databricks clusters.
Data loading from local system to DBFS
1. Select data tab on the left panel and click on Create Table 
![1](https://github.com/kipibi/Pyspark-to-Snowpark-Migration/blob/main/images/working_with_databricks/1.png)

2. click on **Upload File tab** and upload file from local to Databricks
![2](https://github.com/kipibi/Pyspark-to-Snowpark-Migration/blob/main/images/working_with_databricks/2.png)

## Cluster creation
Create a new cluster in Databricks or use an existing cluster. 

Before creating a new cluster, check for existing clusters in the **Clusters** tab of the Databricks portal. If there is an existing cluster, you can restart the cluster.

1. Click **Create** and choose cluster as shown in the image below: 
![3](https://github.com/kipibi/Pyspark-to-Snowpark-Migration/blob/main/images/working_with_databricks/3.png)

2. Enter a name for the cluster.
   You can continue with the default values for Worker type and Driver type.
3. Click **Create** Cluster and wait for the cluster to be up.

**NOTE:**  *If you are using an existing cluster, make sure that the cluster is up and running.*

## Notebook Creation 
1. Click **Create** in the sidebar and select Notebook from the menu. The Create Notebook dialog appears.
![4](https://github.com/kipibi/Pyspark-to-Snowpark-Migration/blob/main/images/working_with_databricks/4.png)

2. Enter a name and select the notebook’s default language.
3. If there are running clusters, the **Cluster** drop-down displays. Select the cluster you want to attach the notebook to.
4. Click **Create**.

## IMPORTING SAMPLE CODE FROM LOCAL

1. To import code from local you can go to FILE→IMPORT NOTEBOOK→FILE UPLOAD and you are ready to execute the code 
![5](https://github.com/kipibi/Pyspark-to-Snowpark-Migration/blob/main/images/working_with_databricks/5.png)

![6](https://github.com/kipibi/Pyspark-to-Snowpark-Migration/blob/main/images/working_with_databricks/6.png)

Now you just need to press the run button in each cell and execute code. 

## IMPORTING SAMPLE CODE FROM GITHUB

1. Go to github repository and copy the URL of the notebook containing the code
![7](https://github.com/kipibi/Pyspark-to-Snowpark-Migration/blob/main/images/working_with_databricks/7.png)

2. To import code directly from github, go to FILE→Import notebook→URL (put the URL of notebook) and click import.
![8](https://github.com/kipibi/Pyspark-to-Snowpark-Migration/blob/main/images/working_with_databricks/8.png) 


