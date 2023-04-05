# Databricks notebook source
# MAGIC %md This notebook sets up the companion cluster(s) to run the solution accelerator. It also creates the Workflow to illustrate the order of execution. Happy exploring! 
# MAGIC 🎉
# MAGIC 
# MAGIC **Steps**
# MAGIC 1. Simply attach this notebook to a cluster with DBR 11.0 and above, and hit Run-All for this notebook. A multi-step job and the clusters used in the job will be created for you and hyperlinks are printed on the last block of the notebook. 
# MAGIC 
# MAGIC 2. Run the accelerator notebooks: Feel free to explore the multi-step job page and **run the Workflow**, or **run the notebooks interactively** with the cluster to see how this solution accelerator executes. 
# MAGIC 
# MAGIC     2a. **Run the Workflow**: Navigate to the Workflow link and hit the `Run Now` 💥. 
# MAGIC   
# MAGIC     2b. **Run the notebooks interactively**: Attach the notebook with the cluster(s) created and execute as described in the `job_json['tasks']` below.
# MAGIC 
# MAGIC **Prerequisites** 
# MAGIC 1. You need to have cluster creation permissions in this workspace.
# MAGIC 
# MAGIC 2. In case the environment has cluster-policies that interfere with automated deployment, you may need to manually create the cluster in accordance with the workspace cluster policy. The `job_json` definition below still provides valuable information about the configuration these series of notebooks should run with. 
# MAGIC 
# MAGIC **Notes**
# MAGIC 1. The pipelines, workflows and clusters created in this script are not user-specific. Keep in mind that rerunning this script again after modification resets them for other users too.
# MAGIC 
# MAGIC 2. If the job execution fails, please confirm that you have set up other environment dependencies as specified in the accelerator notebooks. Accelerators may require the user to set up additional cloud infra or secrets to manage credentials. 

# COMMAND ----------

# DBTITLE 0,Install util packages
# MAGIC %pip install git+https://github.com/databricks-academy/dbacademy@v1.0.13 git+https://github.com/databricks-industry-solutions/notebook-solution-companion@safe-print-html --quiet --disable-pip-version-check

# COMMAND ----------

from solacc.companion import NotebookSolutionCompanion

# COMMAND ----------

from dbacademy.dbgems import get_username
docker_username = dbutils.secrets.get("projectglow", "docker_username") # this secret scope is set up to enable testing only in Databricks' internal environment; please set up secret scope with your own credential
docker_password = dbutils.secrets.get("projectglow", "docker_password") # this secret scope is set up to enable testing only in Databricks' internal environment; please set up secret scope with your own credential
job_json = {
        "name": "gwas-glow-proton-benchmark-test",
        "timeout_seconds": 0,
        "tags":{
          "usage": "glow_benchmarking",
          "group": "HLS"
        },
        "email_notifications": {},
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "job_cluster_key": "gwas_glow_integration_test_photon",
                "notebook_task": {
                    "notebook_path": "etl/data/launch_benchmark",
                    "source": "WORKSPACE"
                },
                "timeout_seconds": 0,
                "email_notifications": {}
            },
            {
                "job_cluster_key": "gwas_single_node",
                "notebook_task": {
                    "notebook_path": f"etl/1_simulate_covariates_phenotypes_offset"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "simulate_covariates_phenotypes_offset",
                "depends_on": [
                    {
                        "task_key": "launch_benchmark"
                    }
                ]
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_photon",
                "notebook_task": {
                    "notebook_path": f"etl/2_simulate_delta_pvcf"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "simulate_delta_pvcf",
                "depends_on": [
                    {
                        "task_key": "launch_benchmark"
                    }
                ]
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_photon",
                "notebook_task": {
                    "notebook_path": f"etl/3_delta_to_vcf"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "delta_to_vcf",
                "depends_on": [
                    {
                        "task_key": "simulate_delta_pvcf"
                    }
                ]
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_photon",
                "notebook_task": {
                    "notebook_path": f"tertiary/pipe-transformer-plink"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "pipe_transformer_plink",
                "depends_on": [
                    {
                        "task_key": "delta_to_vcf"
                    }
                ]
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_photon",
                "notebook_task": {
                    "notebook_path": f"tertiary/parallel_bcftools_filter"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "parallel_bcftools_filter",
                "depends_on": [
                    {
                        "task_key": "glow_logistic_gwas"
                    }
                ]
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_standard",
                "notebook_task": {
                    "notebook_path": f"tertiary/0_ingest_vcf2delta"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "ingest_vcf2delta",
                "depends_on": [
                    {
                        "task_key": "delta_to_vcf"
                    }
                ]
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_standard",
                "notebook_task": {
                    "notebook_path": f"tertiary/1_quality_control"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "quality_control",
                "depends_on": [
                    {
                        "task_key": "ingest_vcf2delta"
                    }
                ]
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_photon",
                "notebook_task": {
                    "notebook_path": f"etl/6_explode_variant_dataframe"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "explode_variants_for_querying",
                "depends_on": [
                    {
                        "task_key": "quality_control"
                    }
                ]
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_standard",
                "notebook_task": {
                    "notebook_path": f"etl/7_etl_gff_annotations"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "generate_gff3_annotations"
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_photon",
                "notebook_task": {
                    "notebook_path": f"etl/8_create_database_for_querying"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "create_database_for_querying",
                "depends_on": [
                    {
                        "task_key": "generate_gff3_annotations"
                    },
                    {
                        "task_key": "explode_variants_for_querying"
                    }
                ]
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_photon",
                "notebook_task": {
                    "notebook_path": f"etl/9_query_variant_db"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "query_variant_database",
                "depends_on": [
                    {
                        "task_key": "create_database_for_querying"
                    }
                ]
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_photon",
                "notebook_task": {
                    "notebook_path": f"tertiary/2_quantitative_glowgr"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "glowgr_quantitative",
                "depends_on": [
                    {
                        "task_key": "simulate_covariates_phenotypes_offset"
                    },
                    {
                        "task_key": "quality_control"
                    }
                ]
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_standard",
                "notebook_task": {
                    "notebook_path": f"tertiary/3_linear_gwas_glow",
                    "base_parameters": {
                        "user": get_username() # to pass user email into R
                    }
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "glow_linear_gwas",
                "depends_on": [
                    {
                        "task_key": "glowgr_quantitative"
                    }
                ]
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_standard",
                "notebook_task": {
                    "notebook_path": f"tertiary/4_binary_glowgr"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "glowgr_binary",
                "depends_on": [
                    {
                        "task_key": "glow_linear_gwas"
                    }
                ]
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_photon",
                "notebook_task": {
                    "notebook_path": f"tertiary/5_logistic_gwas_glow"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "glow_logistic_gwas",
                "depends_on": [
                    {
                        "task_key": "glowgr_binary"
                    }
                ]
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_photon",
                "notebook_task": {
                    "notebook_path": "etl/launch_etl",
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "launch_etl_queries",
                "depends_on": [
                    {
                        "task_key": "generate_gff3_annotations"
                    },
                    {
                        "task_key": "explode_variants_for_querying"
                    }
                ]
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_photon",
                "task_key": "sample_qc_queries",
                "notebook_task": {
                    "notebook_path": "etl/launch_etl",
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                    {
                        "task_key": "launch_etl_queries"
                    }
                ],
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_photon",
                "task_key": "normalize_variants_query",
                "notebook_task": {
                    "notebook_path": "etl/normalizevariants",
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                    {
                        "task_key": "launch_etl_queries"
                    }
                ]
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_photon",
                "task_key": "split_multialellics_queries",
                "depends_on": [
                    {
                        "task_key": "launch_etl_queries"
                    }
                ],
                "notebook_task": {
                    "notebook_path": "etl/splitmultiallelics-transformer",
                },
                "timeout_seconds": 0,
                "email_notifications": {},
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_photon",
                "task_key": "variant_qc_queries",
                "notebook_task": {
                    "notebook_path": "etl/variant-qc-demo",
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                    {
                        "task_key": "launch_etl_queries"
                    }
                ]
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_photon",
                "task_key": "variant_data_queries",
                "notebook_task": {
                    "notebook_path": "etl/variant-data",
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                    {
                        "task_key": "launch_etl_queries"
                    }
                ]
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_photon",
                "task_key": "convert_vcf_to_delta",
                "notebook_task": {
                    "notebook_path": "etl/vcf2delta",
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                    {
                        "task_key": "launch_etl_queries"
                    }
                ]
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_photon",
                "task_key": "binary_glowgr_queries",
                "notebook_task": {
                    "notebook_path": "tertiary/binaryglowgr",
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                    {
                        "task_key": "glow_logistic_gwas"
                    }
                ]
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_photon",
                "task_key": "glowgr_queries",
                "notebook_task": {
                    "notebook_path": "tertiary/glowgr",
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                    {
                        "task_key": "glow_logistic_gwas"
                    }
                ],
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_photon",
                "task_key": "gwas_binary_queries",
                "notebook_task": {
                    "notebook_path": "tertiary/gwas-binary",
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                    {
                        "task_key": "glow_logistic_gwas"
                    }
                ]
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_photon",
                "task_key": "gwas_quantitative_queries",
                "notebook_task": {
                    "notebook_path": "tertiary/gwas-quantitative",
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                    {
                        "task_key": "glow_logistic_gwas"
                    }
                ]
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_photon",
                "task_key": "pandas_linear_mixed_model_queries",
                "notebook_task": {
                    "notebook_path": "tertiary/pandas-lmm",
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                    {
                        "task_key": "glow_logistic_gwas"
                    }
                ]
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_photon",
                "task_key": "parallel_bcftools_filter_queries",
                "notebook_task": {
                    "notebook_path": "tertiary/parallel_bcftools_filter",
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                    {
                        "task_key": "glow_logistic_gwas"
                    }
                ]
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_photon",
                "task_key": "gwas_catalog",
                "notebook_task": {
                    "notebook_path": "dbsql/gwas_catalog_dbsql",
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                    {
                        "task_key": "launch_etl_queries"
                    }
                ]
            }

        ],
        "job_clusters": [
            {
                "job_cluster_key": "gwas_glow_integration_test_photon",
                "new_cluster": {
                    "cluster_name": "gwas_glow_integration_test_photon",
                    "spark_version": "10.4.x-scala2.12",
                    "spark_conf": {
                        "spark.databricks.delta.preview.enabled": "true",
                        "spark.databricks.photon.enabled": "true",
                        "spark.databricks.photon.parquetWriter.enabled": "true",
                        "spark.databricks.photon.scan.enabled": "true",
                        "spark.databricks.photon.sort.enabled": "true",
                        "spark.databricks.photon.window.enabled": "true",
                        "spark.databricks.streaming.forEachBatch.optimized.enabled": "true",
                        "spark.databricks.streaming.forEachBatch.optimized.fastPath.enabled": "true",
                        "spark.databricks.photon.allDataSources.enabled": "true",
                        "spark.databricks.photon.photonRowToColumnar.enabled": "true"
                    },
                    "node_type_id": {"AWS": "i4i.16xlarge", "MSA": "Standard_L16as_v3", "GCP": "n2-highmem-16"},
                    "custom_tags": {
                        "project": "glow",
                        "domain": "genomics",
                        "function": "GWAS",
                        "group": "HLS"
                    },
                    "enable_elastic_disk": "true",
                    "docker_image": {
                        "url": "projectglow/databricks-glow:1.2.1",
                        "basic_auth": {
                            "username": docker_username,
                            "password": docker_password
                        }
                    },
                    "data_security_mode": "NONE",
                    "runtime_engine": "PHOTON",
                    "num_workers": 32
                }
            },
            {
                "job_cluster_key": "gwas_glow_integration_test_standard",
                "new_cluster": {
                    "cluster_name": "gwas_glow_integration_test_standard",
                    "spark_version": "10.4.x-scala2.12",
                    "spark_conf": {
                        "spark.databricks.delta.preview.enabled": "true"
                    },
                    "node_type_id": {"AWS": "i4i.32xlarge", "MSA": "Standard_L32as_v3", "GCP": "n2-highmem-32"},
                    "custom_tags": {
                        "project": "glow",
                        "domain": "genomics",
                        "function": "GWAS",
                        "group": "HLS"
                    },
                    "enable_elastic_disk": "true",
                    "docker_image": {
                        "url": "projectglow/databricks-glow:1.2.1",
                        "basic_auth": {
                            "username": docker_username,
                            "password": docker_password
                        }
                    },
                    "data_security_mode": "NONE",
                    "num_workers": 12
                }
            },            
            {
                "job_cluster_key": "gwas_single_node",
                "new_cluster": {
                    "cluster_name": "gwas_single_node",
                    "spark_version": "10.4.x-scala2.12",
                    "spark_conf": {
                        "spark.databricks.delta.preview.enabled": "true",
                        "spark.databricks.photon.enabled": "true",
                        "spark.databricks.photon.parquetWriter.enabled": "true",
                        "spark.databricks.photon.scan.enabled": "true",
                        "spark.databricks.photon.sort.enabled": "true",
                        "spark.databricks.photon.window.enabled": "true",
                        "spark.databricks.streaming.forEachBatch.optimized.enabled": "true",
                        "spark.databricks.streaming.forEachBatch.optimized.fastPath.enabled": "true",
                        "spark.databricks.photon.allDataSources.enabled": "true",
                        "spark.databricks.photon.photonRowToColumnar.enabled": "true"
                    },
                    "node_type_id": {"AWS": "i4i.4xlarge", "MSA": "Standard_DS4_v2", "GCP": "n1-highmem-4"},
                    "custom_tags": {
                        "project": "glow",
                        "domain": "genomics",
                        "function": "GWAS",
                        "group": "HLS"
                    },
                    "enable_elastic_disk": "true",
                    "docker_image": {
                        "url": "projectglow/databricks-glow:1.2.1",
                        "basic_auth": {
                            "username": docker_username,
                            "password": docker_password
                        }
                    },
                    "data_security_mode": "NONE",
                    "runtime_engine": "PHOTON",
                    "num_workers": 0
                }
            }
        ]
}
  

# COMMAND ----------

dbutils.widgets.dropdown("run_job", "False", ["True", "False"])
run_job = dbutils.widgets.get("run_job") == "True"
NotebookSolutionCompanion().deploy_compute(job_json, run_job=run_job)
