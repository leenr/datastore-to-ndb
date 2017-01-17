import json
import logging
from sys import argv

from google.cloud import datastore

from google.appengine.ext import ndb
from google.appengine.datastore.entity_pb import EntityProto, PropertyValue_ReferenceValue, Reference
from google.net.proto import ProtocolBuffer


PUT_BATCH_SIZE = 100


def convert_protobuf_key_element(path_element, pb2_path_element):
    pb2_path_element.set_type(str(path_element.kind))
    if path_element.name:
        pb2_path_element.set_name(path_element.name.encode('utf8'))
    else:
        pb2_path_element.set_id(path_element.id)
    return pb2_path_element


def convert_protobuf_entity_key(key, pb2_key=None):
    if pb2_key is None:
        pb2_key = Reference()

    pb2_key.set_app(str(key.partition_id.project_id))
    pb2_key.set_name_space(str(key.partition_id.namespace_id))
    pb2_path = pb2_key.mutable_path()
    for path_element in key.path:
        pb2_path_element = pb2_path.add_element()
        convert_protobuf_key_element(path_element, pb2_path_element)

    return pb2_key


def convert_protobuf_value_key(key, pb2_key=None):
    if pb2_key is None:
        pb2_key = PropertyValue_ReferenceValue()

    pb2_key.set_app(str(key.partition_id.project_id))
    pb2_key.set_name_space(str(key.partition_id.namespace_id))
    for path_element in key.path:
        pb2_path_element = pb2_key.add_pathelement()
        convert_protobuf_key_element(path_element, pb2_path_element)

    return pb2_key


def convert_protobuf_user_entity(user_enitity, pb2_user_entity):
    pb2_user_entity.set_email(user_enitity.properties['email'].string_value)
    pb2_user_entity.set_auth_domain(user_enitity.properties['auth_domain'].string_value)
    pb2_user_entity.set_obfuscated_gaiaid(user_enitity.properties['user_id'].string_value)


def convert_protobuf_entity(pb3):
    pb2 = EntityProto()

    convert_protobuf_entity_key(pb3.key, pb2.key())

    for name, full_prop in pb3.properties.iteritems():
        if full_prop.array_value.values:
            is_multiple = True
            properties = full_prop.array_value.values
        else:
            is_multiple = False
            properties = [full_prop]

        for prop in properties:
            if not prop.exclude_from_indexes:
                pb2_prop = pb2.add_property()
            else:
                pb2_prop = pb2.add_raw_property()

            if prop.meaning:
                pb2_prop.set_meaning(prop.meaning)

            if is_multiple:
                pb2_prop.set_multiple(is_multiple)

            pb2_prop.set_name(name)
            pb2_value = pb2_prop.mutable_value()
            for field_descriptor, field in prop.ListFields():
                field_name = field_descriptor.name
                if field_name.endswith('_value'):
                    type_name = field_name[:-len('_value')]
                    value = getattr(prop, field_name)
                    if type_name == 'timestamp':
                        timestamp = value.seconds * 10 ** 6 + value.nanos // 10 ** (9 - 6)
                        pb2_value.set_int64value(timestamp)
                        pb2_prop.set_meaning(pb2_prop.GD_WHEN)
                    elif type_name == 'null':
                        pb2_prop.clear_value()
                    elif type_name == 'key':
                        pb2_referencevalue = pb2_value.mutable_referencevalue()
                        convert_protobuf_value_key(value, pb2_referencevalue)
                    elif type_name == 'entity':
                        if prop.meaning != 20:
                            raise NotImplementedError('Non-user embedded entities is not supported by the NDB')
                        pb2_uservalue = pb2_value.mutable_uservalue()
                        convert_protobuf_user_entity(value, pb2_uservalue)
                    elif type_name == 'array':
                        assert len(value.values) == 0
                        pb2_prop.set_meaning(pb2_prop.EMPTY_LIST)
                    else:
                        type_name = {
                            'integer': 'int64'
                        }.get(type_name, type_name)
                        getattr(pb2_value, 'set_{}value'.format(type_name))(value)

    return pb2


project = argv[1]
kinds = argv[2].split(',')

client = datastore.Client(project=project)
for kind in kinds:
    print('Copying {}...'.format(kind))

    kind_stat_query = client.query(kind='__Stat_Kind__', )
    kind_stat_query.add_filter('kind_name', '=', kind)
    kind_stats = list(kind_stat_query.fetch(1))

    kind_stat = None
    entities_count = None
    if len(kind_stats) > 0:
        kind_stat = kind_stats[0]
        entities_count = kind_stat['count']

    query = client.query(kind=kind)
    iterator = query.fetch()

    # Patch _item_to_value of google.cloud.iterator.Iterator to call convert_protobuf_entity
    #   for each protobuf object returned by Google Datastore
    model = type(str(kind), (ndb.Expando,), {})
    iterator._item_to_value = lambda parent, pb: model._from_pb(convert_protobuf_entity(pb))

    put_batch = []
    for i, ndb_entity in enumerate(iterator):
        put_batch.append(ndb_entity)
        if len(put_batch) >= PUT_BATCH_SIZE:
            ndb.put_multi(put_batch, use_cache=False, use_memcache=False)
            del put_batch[:]
            if entities_count:
                copied = i + 1
                print('  Estimated completion: {:.2f}% ({}/{})'.format(float(copied) / entities_count * 100, copied, entities_count))

    ndb.put_multi(put_batch, use_cache=False, use_memcache=False)
