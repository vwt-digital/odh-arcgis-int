from hashlib import sha256

from google.cloud import firestore


class FirestoreService:
    def __init__(self, kind):
        """
        Initiates the FirestoreService
        """

        self.kind = kind
        self.fs_client = firestore.Client()

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

    def get_entity(self, entity_id):
        """
        Get a Firestore entity

        :param entity_id: Entity ID
        :type entity_id: string

        :return: Entity
        :rtype: dict
        """

        entity_id_hash = self.hash_id(entity_id)

        doc_ref = self.fs_client.collection(self.kind).document(entity_id_hash)
        doc = doc_ref.get()

        if doc.exists:
            return doc.to_dict()

        return None

    def set_entity(self, entity_id, entity_dict):
        """
        Get a Firestore entity

        :param entity_id: Entity ID
        :type entity_id: string
        :param entity_dict: Entity object
        :type entity_dict: dict
        """

        entity_id_hash = self.hash_id(entity_id)

        doc_ref = self.fs_client.collection(self.kind).document(entity_id_hash)
        doc_ref.set(entity_dict)
