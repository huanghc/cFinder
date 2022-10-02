# CFinder

This repo is for code release of our paper `Protecting Data Integrity of Web Applications with Database Constraints Inferred from Application Code.` in `ASPLOS 2023`.   

In the paper we developed a static analysis tool to infer the missing database constraints from the application source code.  
Its workflow contains three steps:
* With the application code as input, CFinder applies the proposed static analysis to find the code snippets that match the conditions of code patterns with assumptions on database constraints.
* From the found snippets, CFinder extracts and infers the formal DB constraints.
* After comparing them with the existing database schema, CFinder outputs the set of missing database constraints.

This repo contains the source code, data, and other artifacts. These are required to reproduce the results we presented in the paper.
It helps the easy reproduction of all the key evaluations in the section 4 of the paper. 

The artifact is available on GitHub at https://github.com/huanghc/cFinder.

## Software dependencies
- Linux (we tested on Ubuntu 18.04)
- Python >=3.8 with packages in requirements.txt. 

## Data sets
- The artifact evaluates seven open-source web applications. Our scripts will automatically download their source code from GitHub. 
- The directory `data` in the artifact includes: 
  - The files containing the database constraints and schema of these web applications. These data are used to generate the main results. 
  - The similar files containing the database constraints and schema, and the source code containing patterns for the history issues (in the directory `data/history_issues`}). These data are used for Table 9 only.

## Steps to reproduce 
- We provide a `make install` command to automatically finish the installation. 
- We provide a `make run_all` command to automatically perform the evaluation. 

```python
make install
# Step 0: Clean the environment, result, app code
# Step 1: Pull application code
# Step 2: Set CFinder Python Envirnment 

make run_all
# Step 3: Run CFinder - the static code analysis tool. 
# Step 4: Run CFinder on history issues
```

## Evaluation and expected results
We provide the scripts to automate the evaluation and generate the Tables and numbers in Section 4. 
The output will be in the `result/` folder and contain the following key results:
- `result/table_4_total_detected_num.csv`: 
  - Total number of detected existing and missing database constraints from each application. (`Table 4`) 
- `result/table_6_breakdown_detected_missing_constraints.csv`: 
  - The breakdown of the number of detected missing database constraints for each constraint type. (`Table 6`)
- `result/table_8_percentage_existing_constraints_already_set_covered.csv`: 
  - The percentage of existing constraints already set in the database that CFinder can cover. (`Table 8`)
- `result/table_9_percentage_constraints_in_collected_dataset_covered.csv`: 
  - The percentage of missing constraints in the collected dataset that CFinder can cover. (`Table 9`)
- `result/table_10_time_to_run_analysis.csv`: 
  - Time (seconds) to run the static analysis. (`Table 10`)

More detailed results for the detected database constraints of each application and each constraint type:
- In the `result/APP_NAME/` directory, 
  - (1) `newly_detected.csv` contains all the newly detected constraints with their code pattern information.
  - (2) `existing_constraints.csv` contains the existing constraints in the database that CFinder can cover.
- In the `result_history_issues/` directory: 
  - Contains the details about each application's constraints that CFinder can detect in the collected dataset of history issues. 

We also provide the list of confirmed issues in `data/acked_issues.csv`. 

Note that some results involve human inspection (`Table 7`) and developers' confirmation (last column in `Table 4`), thus not included in the artifact. 
Note that due to the differences in hardware environments, the performance results in `Table 10` can be different from the numbers reported in the paper.