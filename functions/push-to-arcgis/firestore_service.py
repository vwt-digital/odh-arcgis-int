from google.cloud import firestore


class FirestoreService:
    def __init__(self, kind):
        """
        Initiates the FirestoreService
        """

        self.kind = kind
        self.fs_client = firestore.Client()

    def get_entity(self, entity_id):
        """
        Get a Firestore entity

        :param entity_id: The entity ID
        :type entity_id: string

        :return: Entity
        :rtype: dict
        """

        doc_ref = self.fs_client.collection(self.kind).document(entity_id)
        doc = doc_ref.get()

        if doc.exists:
            return doc.to_dict()

        return None

    def set_entity(self, entity_id, entity_dict):
        """
        Get a Firestore entity

        :param entity_id: The entity ID
        :type entity_id: string
        :param entity_dict: The entity object
        :type entity_dict: dict
        """

        doc_ref = self.fs_client.collection(self.kind).document(entity_id)
        doc_ref.set(entity_dict)
