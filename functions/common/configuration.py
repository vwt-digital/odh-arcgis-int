import yaml


class Configuration:
    """
    Class that holds configuration variables.
    """

    def __init__(self):
        self._configuration = self._read()

    @staticmethod
    def _read() -> dict:
        """
        Reads configuration from a config.py file.
        """

        with open("config.yaml") as f:
            configuration = yaml.safe_load(f)

        return configuration

    @property
    def data_source(self):
        """Incoming message data source."""
        return self._configuration.get("data_source", None)

    @property
    def debug_logging(self):
        """Enable/disable debug logging."""
        return self._configuration.get("debug_logging", False)

    @property
    def existence_check(self):
        """Enable existence check."""
        content = self._configuration.get("existence_check", None)
        return ExistenceCheckConfiguration(content)

    @property
    def high_workload(self):
        """Enable high workload optimisation."""
        return self._configuration.get("high_workload", False)

    @property
    def arcgis_auth(self):
        """ArcGIS authentication configuration"""
        content = self._configuration.get("arcgis", {})
        return ArcGISAuthConfiguration(content)

    @property
    def arcgis_feature_service(self):
        """ArcGIS feature service configuration"""
        content = self._configuration.get("arcgis", {})
        return ArcGISFeatureServiceConfiguration(content)

    @property
    def mapping(self):
        """Field mapping configuration"""
        content = self._configuration.get("mapping", {})
        return MappingConfiguration(content)


class ArcGISAuthConfiguration:
    """
    Class that holds ArcGIS authentication configuration.

    :state: Dictionary ArcGIS authentication configuration.
    """

    def __init__(self, configuration: dict):
        self._authentication = configuration.get("authentication", {})

    @property
    def url(self):
        """ArcGIS authentication URL."""
        return self._authentication.get("url", None)

    @property
    def username(self):
        """ArcGIS authentication username."""
        return self._authentication.get("username", None)

    @property
    def password(self):
        """ArcGIS authentication password."""
        return self._authentication.get("password", None)

    @property
    def token(self):
        """ArcGIS authentication token."""
        return self._authentication.get("token", None)

    @property
    def request(self):
        """ArcGIS authentication request."""
        return self._authentication.get("request", None)

    @property
    def referer(self):
        """ArcGIS authentication referer."""
        return self._authentication.get("referer", None)


class ArcGISFeatureServiceConfiguration:
    """
    Class that holds ArcGIS Feature Service configuration.

    :state: Dictionary ArcGIS Feature Service configuration.
    """

    def __init__(self, configuration: dict):
        self._feature_service = configuration.get("feature_service", {})

    @property
    def url(self):
        """ArcGIS Feature Service URL."""
        if "url" in self._feature_service:
            return self._feature_service["url"].rstrip("/")

        return None

    @property
    def id(self):
        """ArcGIS Feature Service ID."""
        return self._feature_service.get("id", None)

    @property
    def layers(self):
        """ArcGIS Feature Service layers."""
        return self._feature_service.get("layers", None)


class ExistenceCheckConfiguration:
    """
    Class that holds existence check configuration.

    :state: Dictionary existence check configuration.
    """

    def __init__(self, configuration: dict):
        self._configuration = configuration

    @property
    def firestore(self):
        """Firestore existence check"""
        return self._configuration == "firestore"

    @property
    def arcgis(self):
        """ArcGIS existence check"""
        return self._configuration == "arcgis"

    def value(self):
        """Existence check value"""
        return self._configuration


class MappingConfiguration:
    """
    Class that holds field mapping configuration.

    :state: Dictionary field mapping configuration.
    """

    def __init__(self, configuration: dict):
        self._configuration = configuration

    @property
    def attachments(self):
        """Field mapping ID field."""
        return self._configuration.get("attachments", None)

    @property
    def disable_updated_at(self):
        """Disable the updated_at field addition."""
        return self._configuration.get("disable_updated_at", False)

    @property
    def coordinates(self):
        """Coordinate mapping."""

        if "coordinates" in self._configuration:
            return self.CoordinateConfiguration(self._configuration["coordinates"])

        return None

    @property
    def fields(self):
        """Field mapping."""
        return self._configuration.get("fields", {})

    @property
    def id_field(self):
        """Field mapping ID field."""
        return self._configuration.get("id_field", None)

    @property
    def layer_field(self):
        """Field mapping layer field."""
        return self._configuration.get("layer_field", None)

    class CoordinateConfiguration:
        """
        Class that holds coordinates configuration.

        :state: Dictionary coordinates configuration.
        """

        def __init__(self, configuration: dict):
            self._configuration = configuration

        @property
        def longitude(self):
            """Longitude mapping."""
            return self._configuration.get("longitude", None)

        @property
        def latitude(self):
            """Longitude mapping."""
            return self._configuration.get("latitude", None)

        @property
        def conversion(self):
            """Coordinate conversion type."""
            conversion_types = [
                "default",
                "wgs84-web_mercator",
            ]  # List of supported conversion types
            conversion_type = self._configuration.get("conversion", "default")

            if conversion_type not in conversion_types:
                return None

            return conversion_type
