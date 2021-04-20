import os
import re
import time
import yaml
import json
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Dict, Iterable, List

from datahub.configuration import ConfigModel
from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.source.metadata_common import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp, Status, GlobalTagsClass, TagAssociationClass
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    OtherSchemaClass,
    NullTypeClass,
    NumberTypeClass,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    EditableSchemaMetadataClass,
    EditableSchemaFieldInfoClass,
    StringTypeClass,
    TimeTypeClass,
    UnionTypeClass
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DatasetPropertiesClass,
    MapTypeClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    MetadataChangeEventClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
    DatasetSnapshotClass,
    UpstreamLineageClass,
    UpstreamClass,
    DatasetLineageTypeClass,
    CorpUserInfoClass,
    CorpUserSnapshotClass
)


class MetastoreSourceConfig(ConfigModel):
    env: str = "PROD"
    metastore_yml_path: str
    kitchensink_json_dump: str
    database_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    table_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()


@dataclass
class MetastoreSourceReport(SourceReport):
    tables_scanned = 0
    filtered: List[str] = dataclass_field(default_factory=list)

    def report_table_scanned(self) -> None:
        self.tables_scanned += 1

    def report_table_dropped(self, table: str) -> None:
        self.filtered.append(table)


class MetastoreSource(Source):
    source_config: MetastoreSourceConfig
    report = MetastoreSourceReport()

    def __init__(self, config: MetastoreSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config
        self.report = MetastoreSourceReport()
        self.env = config.env

    @classmethod
    def create(cls, config_dict, ctx):
        config = MetastoreSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        def get_all_tables():
            def load_doc(database, table):
                with open(f"{self.source_config.metastore_yml_path}/{database}/{table}.yml", 'r') as stream:
                    try:
                        return yaml.load(stream, Loader=yaml.FullLoader)
                    except yaml.YAMLError as exception:
                        raise exception

            def get_tables_from_database(database: str):
                all_tables_data: List = []

                all_tables = [re.sub('\.yml$', '', f.name) for f in os.scandir(f"{self.source_config.metastore_yml_path}/{database}") if f.is_file()]
                for table in all_tables:
                    if table != ".DS_Store":
                        table_data = load_doc(database, table)
                        all_tables_data.append(table_data)

                return all_tables_data

            all_tables_data: List = []
            if self.source_config.database_pattern.is_fully_specified_allow_list():
                allowed_database_names = self.source_config.database_pattern.get_allowed_list()
                for database in allowed_database_names:
                    all_tables_data += get_tables_from_database(database)
            else:
                all_databases = [f.name for f in os.scandir(self.source_config.metastore_yml_path) if f.is_dir()]
                for database in all_databases:
                    all_tables_data += get_tables_from_database(database)
            return all_tables_data

        def create_owner_entity_mce(owner: str) -> MetadataChangeEventClass:
            return MetadataChangeEventClass(
                proposedSnapshot=CorpUserSnapshotClass(
                    urn=f"urn:li:corpuser:{owner}",
                    aspects=[
                        CorpUserInfoClass(
                            active=True,
                            displayName=owner,
                            fullName=owner,
                            email=f"{owner}@scribd.com"
                        )
                    ],
                )
            )

        def create_global_tags_aspect_mce(table: str, tag: str) -> MetadataChangeEventClass:
            return MetadataChangeEventClass(
                proposedSnapshot=DatasetSnapshotClass(
                    urn=f"urn:li:dataset:(urn:li:dataPlatform:metastore,{table},{self.env})",
                    aspects=[GlobalTagsClass(tags=[TagAssociationClass(tag=f"urn:li:tag:{tag}")])],
                )
            )

        def get_upstream_tables_for_table(table: str) -> [str]:
            def load_docs():
                with open(self.source_config.kitchensink_json_dump) as json_file:
                    return json.load(json_file)

            def get_owner_task():
                for task in load_docs():
                    output_tables = task.get('output_tables')
                    if output_tables is not None and len(output_tables) > 0 and table in output_tables:
                        return task

            def get_task_by_task_id(task_id: str):
                for task in load_docs():
                    if task_id == task['task_id']:
                        return task

            upstream_tables = set()
            task = get_owner_task()
            if task is not None:
                print(f"FOUND owner task: \"{task['task_id']}\" for \"{table}\" table.")
                upstream_task_ids = task.get('upstream_task_ids')
                if upstream_task_ids is not None and 0 < len(upstream_task_ids):
                    for task_id in upstream_task_ids:
                        upstream_task = get_task_by_task_id(task_id)
                        output_tables = upstream_task.get('output_tables')
                        #TODO: restore all lineage dependency (now added link only to task that has one table as output)
                        if output_tables is not None and 0 < len(output_tables) < 2:
                            for output_table in output_tables:
                                upstream_tables.add(output_table)

            print(f"FOUND upstream_tables: \"{str(upstream_tables)}\" for \"{table}\" table.")
            return upstream_tables

        def create_lineage_aspect_mce(table: str, upstreams: [str]) -> MetadataChangeEventClass:
            return MetadataChangeEventClass(
                proposedSnapshot=DatasetSnapshotClass(
                    urn=f"urn:li:dataset:(urn:li:dataPlatform:metastore,{table},{self.env})",
                    aspects=[
                        UpstreamLineageClass(
                            upstreams=[
                                UpstreamClass(
                                    dataset=f"urn:li:dataset:(urn:li:dataPlatform:metastore,{upstream},{self.env})",
                                    type=DatasetLineageTypeClass.TRANSFORMED,
                                    auditStamp=AuditStampClass(
                                        time=int(time.time() * 1000),
                                        actor="urn:li:corpuser:datahub",
                                    )
                                )
                                for upstream in upstreams
                            ]
                        )
                    ],
                )
            )

        tables = get_all_tables()
        owners = set()
        for table in tables:
            owner = table.get("owner")
            if owner != "UNASSIGNED":
                owners.add(owner)

        # create owners
        for owner in owners:
            work_unit = MetadataWorkUnit(id=f"User-{owner}", mce=create_owner_entity_mce(owner))
            self.report.report_workunit(work_unit)
            yield work_unit

        for table in tables:
            print("DATA: " + json.dumps(table))

            table_name = table["table_name"]
            database_name = table_name.split(".")[0]
            self.report.report_table_scanned()
            if not self.source_config.database_pattern.allowed(
                database_name
            ) or not self.source_config.table_pattern.allowed(table_name):
                self.report.report_table_dropped(table_name)
                continue

            mce = self._extract_record(table)
            work_unit = MetadataWorkUnit(id=f"Metastore-{table_name}", mce=mce)
            self.report.report_workunit(work_unit)
            yield work_unit

            tag_work_unit = MetadataWorkUnit(id=f"DELTA-Tag-{table_name}", mce=create_global_tags_aspect_mce(table_name, "DELTA"))
            self.report.report_workunit(tag_work_unit)
            yield tag_work_unit

            if table.get("is_streaming"):
                tag_work_unit = MetadataWorkUnit(id=f"STREAMING-Tag-{table_name}", mce=create_global_tags_aspect_mce(table_name, "STREAMING"))
                self.report.report_workunit(tag_work_unit)
                yield tag_work_unit

            upstreams = get_upstream_tables_for_table(table_name)
            if upstreams is not None and len(upstreams) > 0:
                lineage_work_unit = MetadataWorkUnit(id=f"Lineage-{table_name}", mce=create_lineage_aspect_mce(table_name, upstreams))
                self.report.report_workunit(lineage_work_unit)
                yield lineage_work_unit

    def _extract_record(self, table: Dict) -> MetadataChangeEvent:
        def get_owner() -> OwnershipClass:
            owner = table.get("owner")
            if owner:
                owners = [
                    OwnerClass(
                        owner=f"urn:li:corpuser:{owner}",
                        type=OwnershipTypeClass.DATAOWNER,
                    )
                ]
            else:
                owners = []
            return OwnershipClass(
                owners=owners,
                lastModified=AuditStampClass(time=sys_time, actor="urn:li:corpuser:datahub")
            )

        def get_institutional_memory() -> InstitutionalMemoryClass:
            return InstitutionalMemoryClass(
                elements=[
                    InstitutionalMemoryMetadataClass(
                        url="s3://scribd-data-warehouse-prod/databases/" + table["table_name"].replace(".", "/"),
                        description="S3 table location",
                        createStamp=AuditStampClass(time=sys_time, actor="urn:li:corpuser:datahub")
                    ),
                    InstitutionalMemoryMetadataClass(
                        url="https://github.com/scribd/metastore/blob/master/yml/" + table["table_name"].replace(".", "/") + ".yml",
                        description="Source code location",
                        createStamp=AuditStampClass(time=sys_time, actor="urn:li:corpuser:datahub")
                    )
                ]
            )

        def get_dataset_properties() -> DatasetPropertiesClass:
            return DatasetPropertiesClass(
                description=table.get("comment"),
                customProperties={
                    **{
                        "sensitivity_category": str(table.get("sensitivity_category"))
                    },
                    **{
                        "partition_columns": str(table.get("partition_columns"))
                    }
                },
                uri=None,
                tags=[],
            )

        #example: {"table_name": "warehouse.forecast_no_segmentations_weekly_seasonality_20171001", "fields": {"platform": {"type": "string", "comment": "from deserializer"}, "weekday_index": {"type": "string", "comment": "from deserializer"}, "rate_signup_change_rel_to_mon": {"type": "string", "comment": "from deserializer"}}, "partition_columns": [], "sort_columns": [], "sensitivity_category": "GENERAL", "owner": "UNASSIGNED"}
        def get_schema_metadata(metastore_source: MetastoreSource) -> SchemaMetadata:
            table_name = table["table_name"]
            schema = table["fields"]
            fields: List[SchemaField] = []
            for field in schema:
                schema_field = SchemaField(
                    fieldPath=field,
                    nativeDataType=schema[field]["type"],
                    type=get_column_type(
                        metastore_source, schema[field]["type"], table_name, field
                    ),
                    description=schema[field].get("comment"),
                    recursive=False,
                    nullable=True,
                )
                fields.append(schema_field)
            return SchemaMetadata(
                schemaName=table_name,
                version=0,
                fields=fields,
                platform="urn:li:dataPlatform:metastore",
                created=AuditStamp(time=sys_time, actor="urn:li:corpuser:datahub"),
                lastModified=AuditStamp(time=sys_time, actor="urn:li:corpuser:datahub"),
                hash="",
                platformSchema=OtherSchemaClass(rawSchema=str(table)),
            )

        def get_editable_schema_metadata() -> EditableSchemaMetadataClass:
            return EditableSchemaMetadataClass(
                created=AuditStampClass(time=sys_time, actor="urn:li:corpuser:datahub"),
                lastModified=AuditStampClass(time=sys_time, actor="urn:li:corpuser:datahub"),
                editableSchemaFieldInfo=[
                    EditableSchemaFieldInfoClass(
                        fieldPath=str(i),
                        description=None,
                        globalTags=GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:PARTITION_COLUMN")]))
                    for i in table.get("partition_columns")
                ]
            )

        sys_time = int(time.time() * 1000)
        table_name = table["table_name"]
        dataset_snapshot = DatasetSnapshot(
            urn=f"urn:li:dataset:(urn:li:dataPlatform:metastore,{table_name},{self.env})",
            aspects=[],
        )

        dataset_snapshot.aspects.append(Status(removed=False))
        dataset_snapshot.aspects.append(get_owner())
        dataset_snapshot.aspects.append(get_dataset_properties())
        dataset_snapshot.aspects.append(get_institutional_memory())
        dataset_snapshot.aspects.append(get_schema_metadata(self))
        dataset_snapshot.aspects.append(get_editable_schema_metadata())

        metadata_record = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return metadata_record

    def get_report(self):
        return self.report

    def close(self):
        pass


def get_column_type(
        metastore_source: MetastoreSource, field_type: str, table_name: str, field_name: str
) -> SchemaFieldDataType:
    field_type_mapping = {
        "array": ArrayTypeClass,
        "bigint": NumberTypeClass,
        "binary": BytesTypeClass,
        "byte": BytesTypeClass,
        "boolean": BooleanTypeClass,
        "char": StringTypeClass,
        "date": DateTypeClass,
        "decimal": NumberTypeClass,
        "decimal(4,2)": NumberTypeClass,
        "decimal(5,2)": NumberTypeClass,
        "decimal(7,2)": NumberTypeClass,
        "decimal(7,4)": NumberTypeClass,
        "decimal(8,2)": NumberTypeClass,
        "decimal(8,5)": NumberTypeClass,
        "decimal(9,2)": NumberTypeClass,
        "decimal(10,1)": NumberTypeClass,
        "decimal(10,2)": NumberTypeClass,
        "decimal(11,4)": NumberTypeClass,
        "decimal(12,4)": NumberTypeClass,
        "decimal(20,0)": NumberTypeClass,
        "decimal(25,2)": NumberTypeClass,
        "decimal(38,0)": NumberTypeClass,
        "double": NumberTypeClass,
        "float": NumberTypeClass,
        "short": NumberTypeClass,
        "int": NumberTypeClass,
        "integer": NumberTypeClass,
        "interval": TimeTypeClass,
        "long": NumberTypeClass,
        "map": MapTypeClass,
        "null": NullTypeClass,
        "set": ArrayTypeClass,
        "smallint": NumberTypeClass,
        "string": StringTypeClass,
        "struct": MapTypeClass,
        "vector": MapTypeClass,
        "timestamp": TimeTypeClass,
        "tinyint": NumberTypeClass,
        "union": UnionTypeClass,
        "varchar": StringTypeClass,
    }

    if field_type in field_type_mapping.keys():
        type_class = field_type_mapping[field_type]
    elif field_type.startswith("array"):
        type_class = ArrayTypeClass
    elif field_type.startswith("map") or field_type.startswith("struct"):
        type_class = MapTypeClass
    elif field_type.startswith("set"):
        type_class = ArrayTypeClass
    else:
        metastore_source.report.report_warning(
            field_type,
            f"The type '{field_type}' is not recognised for field '{field_name}' in table '{table_name}', setting as StringTypeClass.",
        )
        type_class = StringTypeClass
    data_type = SchemaFieldDataType(type=type_class())
    return data_type
