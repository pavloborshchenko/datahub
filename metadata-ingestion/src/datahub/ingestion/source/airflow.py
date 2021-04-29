import time
import json
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Dict, Iterable, List

from datahub.configuration import ConfigModel
from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.source.metadata_common import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    DataFlowSnapshotClass,
    DataJobSnapshotClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    AuditStampClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass
)


class AirflowSourceConfig(ConfigModel):
    env: str = "PROD"
    kitchensink_json_dump: str
    database_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    table_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()


@dataclass
class AirflowSourceReport(SourceReport):
    tables_scanned = 0
    filtered: List[str] = dataclass_field(default_factory=list)

    def report_table_scanned(self) -> None:
        self.tables_scanned += 1

    def report_table_dropped(self, table: str) -> None:
        self.filtered.append(table)


class AirflowSource(Source):
    source_config: AirflowSourceConfig
    report = AirflowSourceReport()

    def __init__(self, config: AirflowSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config
        self.report = AirflowSourceReport()
        self.env = config.env

    @classmethod
    def create(cls, config_dict, ctx):
        config = AirflowSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        def load_docs():
            with open(self.source_config.kitchensink_json_dump) as json_file:
                return json.load(json_file)

        def get_task_by_task_id(task_id: str):
            for task in load_docs():
                if task_id == task['task_id']:
                    return task

        def get_output_tables_from_upstream_tasks(upsteams: List[str]):
            tables = set()
            for task_id in upsteams:
                job = get_task_by_task_id(task_id)
                output_tables = job["output_tables"]
                if output_tables is not None and 0 < len(output_tables) < 2:
                    for table in output_tables:
                        tables.add(table)
            return tables

        def get_owner(owner: str) -> OwnershipClass:
            return OwnershipClass(
                owners=[
                    OwnerClass(
                        owner=f"urn:li:corpuser:{owner}",
                        type=OwnershipTypeClass.DATAOWNER,
                    )
                ],
                lastModified=AuditStampClass(time=sys_time, actor="urn:li:corpuser:datahub")
            )

        # DataFlowSnapshot -> Dags simulations
        #{
        #   "auditHeader": null,
        #   "proposedSnapshot": {
        #       "com.linkedin.pegasus2avro.metadata.snapshot.DataFlowSnapshot": {
        #           "urn": "urn:li:dataFlow:(airflow,dag_abc,PROD)",
        #           "aspects": [
        #               {
        #                   "com.linkedin.pegasus2avro.common.Ownership": {
        #                       "owners": [
        #                           {
        #                               "owner": "urn:li:corpuser:datahub",
        #                               "type": "DATAOWNER",
        #                               "source": null
        #                           }
        #                       ],
        #                       "lastModified": {
        #                           "time": 1581407189000,
        #                           "actor": "urn:li:corpuser:datahub",
        #                           "impersonator": null
        #                       }
        #                   }
        #               },
        #               {
        #                   "com.linkedin.pegasus2avro.datajob.DataFlowInfo": {
        #                       "name": "Users",
        #                       "description": "Constructs the fct_users_deleted and fct_users_created tables",
        #                       "project": null
        #                   }
        #               }
        #           ]
        #       }
        #   },
        #   "proposedDelta": null
        #}
        def create_data_flow_snapshot(self, dag_name: str) -> MetadataChangeEvent:
            # urn: str,
            # aspects: List[Union["DataFlowInfoClass", "OwnershipClass"]]
            data_flow_snapshot = DataFlowSnapshotClass(
                urn=f"urn:li:dataFlow:(airflow,{dag_name},{self.env})",
                aspects=[]
            )
            data_flow_snapshot.aspects.append(DataFlowInfoClass(
                name=dag_name,
                description=dag_name,
                project="Scribd"
            ))
            data_flow_snapshot.aspects.append(get_owner(owner="data-end"))
            return MetadataChangeEvent(proposedSnapshot=data_flow_snapshot)

        # DataJobSnapshot -> task simulation
        #{
        #    "auditHeader": null,
        #    "proposedSnapshot": {
        #        "com.linkedin.pegasus2avro.metadata.snapshot.DataJobSnapshot": {
        #            "urn": "urn:li:dataJob:(urn:li:dataFlow:(airflow,dag_abc,PROD),task_123)",
        #            "aspects": [
        #                {
        #                    "com.linkedin.pegasus2avro.common.Ownership": {
        #                        "owners": [
        #                            {
        #                                "owner": "urn:li:corpuser:datahub",
        #                                "type": "DATAOWNER",
        #                                "source": null
        #                            }
        #                        ],
        #                        "lastModified": {
        #                            "time": 1581407189000,
        #                            "actor": "urn:li:corpuser:datahub",
        #                            "impersonator": null
        #                        }
        #                    }
        #                },
        #                {
        #                    "com.linkedin.pegasus2avro.datajob.DataJobInfo": {
        #                        "name": "User Creations",
        #                        "description": "Constructs the fct_users_created from logging_events",
        #                        "type": "SQL"
        #                    }
        #                },
        #                {
        #                    "com.linkedin.pegasus2avro.datajob.DataJobInputOutput": {
        #                        "inputDatasets": [
        #                            "urn:li:dataset:(urn:li:dataPlatform:hive,logging_events,PROD)"
        #                        ],
        #                        "outputDatasets": [
        #                            "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"
        #                        ]
        #                    }
        #                }
        #            ]
        #        }
        #    },
        #    "proposedDelta": null
        #}
        def create_data_job_snapshot(self, dag_name: str, job: Dict) -> MetadataChangeEvent:
            #urn: str,
            #aspects: List[Union["DataJobInfoClass", "DataJobInputOutputClass", "OwnershipClass"]]
            job_id = job["task_id"]

            data_job_snapshot = DataJobSnapshotClass(
                urn=f"urn:li:dataJob:(urn:li:dataFlow:(airflow,{dag_name},{self.env}),{job_id})",
                aspects=[]
            )
            data_job_snapshot.aspects.append(DataJobInfoClass(
                name=job_id,
                #TODO:
                #type=job["operator"],
                type="SQL",
                description=job["label"]
            ))
            data_job_snapshot.aspects.append(DataJobInputOutputClass(
                inputDatasets=[
                    f"urn:li:dataset:(urn:li:dataPlatform:metastore,{table},{self.env})"
                    for table in get_output_tables_from_upstream_tasks(job["upstream_task_ids"])
                ],
                outputDatasets=[
                    f"urn:li:dataset:(urn:li:dataPlatform:metastore,{table},{self.env})"
                    for table in job["output_tables"]
                ]
            ))
            data_job_snapshot.aspects.append(get_owner(owner=job["owner"]))
            return MetadataChangeEvent(proposedSnapshot=data_job_snapshot)

        sys_time = int(time.time() * 1000)
        jobs = load_docs()

        dag_name = "kitchensink-etl"
        work_unit = MetadataWorkUnit(id=f"Airflow-{dag_name}-dag", mce=create_data_flow_snapshot(self, dag_name=dag_name))
        self.report.report_workunit(work_unit)
        yield work_unit

        for job in jobs:
            print("DATA: " + json.dumps(job))
            job_id = job["task_id"]

            mce=create_data_job_snapshot(self, dag_name=dag_name, job=job)
            job_work_unit = MetadataWorkUnit(id=f"Airflow-{job_id}-job", mce=mce)
            self.report.report_workunit(job_work_unit)
            yield job_work_unit

    def get_report(self):
        return self.report

    def close(self):
        pass
