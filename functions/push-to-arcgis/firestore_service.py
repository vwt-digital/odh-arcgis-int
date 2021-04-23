import logging
from hashlib import sha256
from itertools import islice

from google.cloud import firestore


class FirestoreService:
    def __init__(self, high_workload, kind):
        """
        Initiates the FirestoreService

        :param high_workload: Has high workload
        :type high_workload: boolean
        :param kind: Firestore Collection kind
        :type kind: str
        """

        self.kind = kind
        self.fs_client = firestore.Client()

        self.entity_list = self.get_all_entities() if high_workload else None
        self.entities_to_save = {}

    @staticmethod
    def hash_id(entity_id):
        """
        Hash ID

        :param entity_id: ID
        :type entity_id: string

        :return: Hashed ID
        :rtype: string
        """

        return sha256(entity_id.encode("utf-8")).hexdigest()

    def get_all_entities(self):
        """
        Get all Firestore entities

        :return: Entities
        :rtype: dict
        """

        entity_list = {}

        query = self.fs_client.collection(self.kind)

        for entity in query.stream():
            entity_list[entity.id] = entity.to_dict()

        logging.info(
            f"Retrieved {len(entity_list)} entities from collection '{self.kind}' for high workload optimization"
        )

        return entity_list

    def save_new_entities(self):
        """
        Save Firestore entities in batch
        """

        updated_entities_count = 0

        for chunk in chunks(self.entities_to_save, 500):  # Batches of max 500 entities
            batch = self.fs_client.batch()

            for entity_id in chunk:
                entity_ref = self.fs_client.collection(self.kind).document(entity_id)
                batch.set(entity_ref, chunk[entity_id])
                updated_entities_count += 1

            batch.commit()

        self.entities_to_save = {}

        logging.info(
            f"Added {updated_entities_count} features to Firestore collection '{self.kind}'"
        )

    def get_entity(self, entity_id):
        """
        Get a Firestore entity

        :param entity_id: Entity ID
        :type entity_id: string

        :return: Entity
        :rtype: dict
        """

        entity_id_hash = self.hash_id(entity_id)

        if not self.entity_list:
            doc_ref = self.fs_client.collection(self.kind).document(entity_id_hash)
            doc = doc_ref.get()

            if doc.exists:
                return doc.to_dict()

        if entity_id_hash in self.entity_list:
            return self.entity_list.get(entity_id_hash)

        return None

    def set_entity(self, entity_id, entity_dict):
        """
        Set a Firestore entity

        :param entity_id: Entity ID
        :type entity_id: string
        :param entity_dict: Entity object
        :type entity_dict: dict
        """

        entity_id_hash = self.hash_id(entity_id)

        self.entities_to_save[entity_id_hash] = entity_dict

        if self.entity_list and entity_id_hash not in self.entity_list:
            self.entity_list[entity_id_hash] = entity_dict

    def close(self):
        """
        Close the service
        """

        # Save new entities to Firestore if available
        if self.entities_to_save:
            self.save_new_entities()


def chunks(data, chunk_size):
    """
    Split a dictionary into chunks

    :param data: Dictionary of data
    :type data: dict
    :param chunk_size: Chunk size
    :type chunk_size: int
    """

    it = iter(data)
    for i in range(0, len(data), chunk_size):
        yield {k: data[k] for k in islice(it, chunk_size)}
