# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

from typing import (
    Iterable, Iterator, Union,
)

from amundsen_common.utils.atlas import (
    AtlasCommonParams, AtlasCommonTypes, AtlasTableTypes,
)
from amundsen_rds.models import RDSModel
from amundsen_rds.models.table import TableUsage as RDSTableUsage

from databuilder.models.atlas_entity import AtlasEntity
from databuilder.models.atlas_relationship import AtlasRelationship
from databuilder.models.atlas_serializable import AtlasSerializable
from databuilder.models.graph_node import GraphNode
from databuilder.models.graph_relationship import GraphRelationship
from databuilder.models.graph_serializable import GraphSerializable
from databuilder.models.table_metadata import TableMetadata
from databuilder.models.table_serializable import TableSerializable
from databuilder.models.user import User
from databuilder.serializers.atlas_serializer import get_entity_attrs
from databuilder.utils.atlas import AtlasRelationshipTypes, AtlasSerializedEntityOperation


class ColumnReader(object):
    """
    A class represent user's read action on column. Implicitly assumes that read count is one.
    """

    def __init__(self,
                 database: str,
                 cluster: str,
                 schema: str,
                 table: str,
                 column: str,
                 user_email: str,
                 read_count: int = 1
                 ) -> None:
        self.database = database
        self.cluster = cluster
        self.schema = schema
        self.table = table
        self.column = column
        self.user_email = user_email
        self.read_count = int(read_count)

    def __repr__(self) -> str:
        return f"ColumnReader(database={self.database!r}, cluster={self.cluster!r}, " \
               f"schema={self.schema!r}, table={self.table!r}, column={self.column!r}, " \
               f"user_email={self.user_email!r}, read_count={self.read_count!r})"


class TableColumnUsage(GraphSerializable, TableSerializable, AtlasSerializable):
    """
    A model represents user <--> column graph model
    Currently it only support to serialize to table level
    """
    TABLE_NODE_LABEL = TableMetadata.TABLE_NODE_LABEL
    TABLE_NODE_KEY_FORMAT = TableMetadata.TABLE_KEY_FORMAT

    USER_TABLE_RELATION_TYPE = 'READ'
    TABLE_USER_RELATION_TYPE = 'READ_BY'

    # Property key for relationship read, readby relationship
    READ_RELATION_COUNT = 'read_count'

    def __init__(self, col_readers: Iterable[ColumnReader]) -> None:
        for col_reader in col_readers:
            if col_reader.column != '*':
                raise NotImplementedError(f'Column is not supported yet {col_readers}')

        self.col_readers = col_readers
        self._node_iterator = self._create_node_iterator()
        self._rel_iter = self._create_rel_iterator()
        self._record_iter = self._create_record_iterator()
        self._atlas_entity_iterator = self._create_next_atlas_entity()
        self._atlas_relation_iterator = self._create_atlas_relation_iterator()

    def create_next_node(self) -> Union[GraphNode, None]:
        try:
            return next(self._node_iterator)
        except StopIteration:
            return None

    def _create_node_iterator(self) -> Iterator[GraphNode]:
        for col_reader in self.col_readers:
            if col_reader.column == '*':
                # using yield for better memory efficiency
                user_node = User(email=col_reader.user_email).get_user_node()
                yield user_node

    def create_next_relation(self) -> Union[GraphRelationship, None]:
        try:
            return next(self._rel_iter)
        except StopIteration:
            return None

    def _create_rel_iterator(self) -> Iterator[GraphRelationship]:
        for col_reader in self.col_readers:
            relationship = GraphRelationship(
                start_label=TableMetadata.TABLE_NODE_LABEL,
                start_key=self._get_table_key(col_reader),
                end_label=User.USER_NODE_LABEL,
                end_key=self._get_user_key(col_reader.user_email),
                type=TableColumnUsage.TABLE_USER_RELATION_TYPE,
                reverse_type=TableColumnUsage.USER_TABLE_RELATION_TYPE,
                attributes={
                    TableColumnUsage.READ_RELATION_COUNT: col_reader.read_count
                }
            )
            yield relationship

    def create_next_record(self) -> Union[RDSModel, None]:
        try:
            return next(self._record_iter)
        except StopIteration:
            return None

    def _create_record_iterator(self) -> Iterator[RDSModel]:
        for col_reader in self.col_readers:
            if col_reader.column == '*':
                user_record = User(email=col_reader.user_email).get_user_record()
                yield user_record

            table_usage_record = RDSTableUsage(user_rk=self._get_user_key(col_reader.user_email),
                                               table_rk=self._get_table_key(col_reader),
                                               read_count=col_reader.read_count)
            yield table_usage_record

    def _get_table_key(self, col_reader: ColumnReader) -> str:
        return TableMetadata.TABLE_KEY_FORMAT.format(db=col_reader.database,
                                                     cluster=col_reader.cluster,
                                                     schema=col_reader.schema,
                                                     tbl=col_reader.table)

    def _get_user_key(self, email: str) -> str:
        return User.get_user_model_key(email=email)

    def _get_entity_type(self) -> str:
        return AtlasTableTypes.table

    def _get_entity_key(self, user: ColumnReader) -> str:
        return self._get_table_key(user)

    def _get_reader_key(self, user: ColumnReader) -> str:
        return f'{self._get_entity_key(user)}/_reader/{user.user_email}'

    def _create_atlas_user_entity(self, user: ColumnReader) -> AtlasEntity:
        attrs_mapping = [
            (AtlasCommonParams.qualified_name, user.user_email),
            ('email', user.user_email)
        ]

        entity_attrs = get_entity_attrs(attrs_mapping)

        entity = AtlasEntity(
            typeName=AtlasCommonTypes.user,
            operation=AtlasSerializedEntityOperation.CREATE,
            attributes=entity_attrs,
            relationships=None
        )

        return entity

    def _create_atlas_reader_entity(self, user: ColumnReader) -> AtlasEntity:
        attrs_mapping = [
            (AtlasCommonParams.qualified_name, self._get_reader_key(user)),
            ('count', user.read_count),
            ('entityUri', self._get_entity_key(user))
        ]

        entity_attrs = get_entity_attrs(attrs_mapping)

        entity = AtlasEntity(
            typeName=AtlasCommonTypes.reader,
            operation=AtlasSerializedEntityOperation.CREATE,
            attributes=entity_attrs,
            relationships=None
        )

        return entity

    def _create_atlas_reader_dataset_relation(self, user: ColumnReader) -> AtlasRelationship:
        relationship = AtlasRelationship(
            relationshipType=AtlasRelationshipTypes.referenceable_reader,
            entityType1=self._get_entity_type(),
            entityQualifiedName1=self._get_entity_key(user),
            entityType2=AtlasCommonTypes.reader,
            entityQualifiedName2=self._get_reader_key(user),
            attributes=dict(count=user.read_count)
        )

        return relationship

    def _create_atlas_user_reader_relation(self, user: ColumnReader) -> AtlasRelationship:
        relationship = AtlasRelationship(
            relationshipType=AtlasRelationshipTypes.reader_user,
            entityType1=AtlasCommonTypes.reader,
            entityQualifiedName1=self._get_reader_key(user),
            entityType2=AtlasCommonTypes.user,
            entityQualifiedName2=self._get_user_key(user.user_email),
            attributes={}
        )

        return relationship

    def _create_next_atlas_entity(self) -> Iterator[AtlasEntity]:
        for col_reader in self.col_readers:
            if col_reader.column == '*':
                yield self._create_atlas_user_entity(col_reader)
                yield self._create_atlas_reader_entity(col_reader)

    def create_next_atlas_entity(self) -> Union[AtlasEntity, None]:
        try:
            return next(self._atlas_entity_iterator)
        except StopIteration:
            return None

    def create_next_atlas_relation(self) -> Union[AtlasRelationship, None]:
        try:
            return next(self._atlas_relation_iterator)
        except StopIteration:
            return None

    def _create_atlas_relation_iterator(self) -> Iterator[AtlasRelationship]:
        for col_reader in self.col_readers:
            if col_reader.column == '*':
                yield self._create_atlas_reader_dataset_relation(col_reader)
                yield self._create_atlas_user_reader_relation(col_reader)

    def __repr__(self) -> str:
        return f'TableColumnUsage(col_readers={self.col_readers!r})'
