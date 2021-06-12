import abc
from typing import Iterator, Union

from amundsen_common.utils.atlas import AtlasCommonParams, AtlasCommonTypes

from databuilder.models.atlas_entity import AtlasEntity
from databuilder.models.atlas_relationship import AtlasRelationship
from databuilder.models.atlas_serializable import AtlasSerializable
from databuilder.serializers.atlas_serializer import get_entity_attrs
from databuilder.utils.atlas import AtlasRelationshipTypes, AtlasSerializedEntityOperation


class AtlasUsage(abc.ABC, AtlasSerializable):
    @abc.abstractmethod
    def _get_entity_type(self):
        pass

    @abc.abstractmethod
    def _get_user_key(self):
        pass

    @abc.abstractmethod
    def _get_entity_key(self):
        pass

    @abc.abstractmethod
    def _get_usage(self):
        pass

    def _get_reader_key(self) -> str:
        return f'{self._get_entity_key()}/_reader/{self._get_user_key()}'

    def _create_atlas_user_entity(self) -> AtlasEntity:
        attrs_mapping = [
            (AtlasCommonParams.qualified_name, self._get_user_key()),
            ('email', self._get_user_key())
        ]

        entity_attrs = get_entity_attrs(attrs_mapping)

        entity = AtlasEntity(
            typeName=AtlasCommonTypes.user,
            operation=AtlasSerializedEntityOperation.CREATE,
            attributes=entity_attrs,
            relationships=None
        )

        return entity

    def _create_atlas_reader_entity(self) -> AtlasEntity:
        attrs_mapping = [
            (AtlasCommonParams.qualified_name, self._get_reader_key()),
            ('count', self._get_usage()),
            ('entityUri', self._get_entity_key())
        ]

        entity_attrs = get_entity_attrs(attrs_mapping)

        entity = AtlasEntity(
            typeName=AtlasCommonTypes.reader,
            operation=AtlasSerializedEntityOperation.CREATE,
            attributes=entity_attrs,
            relationships=None
        )

        return entity

    def _create_atlas_reader_dataset_relation(self) -> AtlasRelationship:
        relationship = AtlasRelationship(
            relationshipType=AtlasRelationshipTypes.referenceable_reader,
            entityType1=self._get_entity_type(),
            entityQualifiedName1=self._get_entity_key(),
            entityType2=AtlasCommonTypes.reader,
            entityQualifiedName2=self._get_reader_key(),
            attributes=dict(count=self._get_usage())
        )

        return relationship

    def _create_atlas_user_reader_relation(self) -> AtlasRelationship:
        relationship = AtlasRelationship(
            relationshipType=AtlasRelationshipTypes.reader_user,
            entityType1=AtlasCommonTypes.reader,
            entityQualifiedName1=self._get_reader_key(),
            entityType2=AtlasCommonTypes.user,
            entityQualifiedName2=self._get_user_key(),
            attributes={}
        )

        return relationship

    def _create_next_atlas_entity(self) -> Iterator[AtlasEntity]:
        yield self._create_atlas_user_entity()
        yield self._create_atlas_reader_entity()

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
        yield self._create_atlas_reader_dataset_relation()
        yield self._create_atlas_user_reader_relation()
