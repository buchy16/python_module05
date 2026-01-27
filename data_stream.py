"""Système polymorphe de flux de données.

Fournit des classes abstraites et concrètes pour traiter des lots de données
de capteurs, de transactions et d'événements, ainsi qu'un processeur unifié
pour orchestrer leur exécution.
"""

from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    """Interface abstraite pour un flux de données.

    Chaque flux doit implémenter le traitement d'un lot, un filtrage optionnel
    et l'exposition de statistiques associées.
    """
    def __init__(self, stream_id: str, type: str):
        """Initialise un flux générique.

        Args:
            stream_id: Identifiant unique du flux.
            type: Type lisible du flux (ex. "Sensor", "Transaction").
        """
        self.stream_id = stream_id
        self.type = type

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        """Traite un lot de données et retourne un résumé textuel.

        Args:
            data_batch: Collection hétérogène d'éléments à traiter.

        Returns:
            Une courte chaîne décrivant le nombre d'éléments traités.
        """
        pass

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        """Filtre les données du lot selon des critères facultatifs.

        Args:
            data_batch: Lot de données à filtrer.
            criteria: Nom d'un filtre optionnel (ex. "High-priority").

        Returns:
            Les éléments valides correspondant au flux et au critère.
        """
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Retourne des statistiques agrégées du flux.

        Returns:
            Dictionnaire de métriques (ex. moyenne, total, compteur).
        """
        pass


class SensorStream(DataStream):
    """Flux dédié aux mesures de capteurs (température, humidité, pression)."""
    def __init__(self, stream_id: str, type: str):
        """Initialise le flux capteur et ses accumulateurs internes.

        Args:
            stream_id: Identifiant unique du flux.
            type: Libellé du type de flux (ex. "Sensor").
        """
        super().__init__(stream_id, type)
        self.sensor_report = 0
        self.avg_t = []

    def process_batch(self, data_batch: List[Any]) -> str:
        """Valide, filtre, puis agrège un lot de mesures capteurs.

        Valide le type des données, ne conserve que les tuples attendus,
        compte les lectures et collecte les températures pour une moyenne.

        Args:
            data_batch: Lot hétérogène de données.

        Returns:
            Chaîne de type "N readings" ou "0 readings" en cas d'erreur.
        """
        try:
            if (isinstance(data_batch, List) is False):
                raise Exception(f"Error data is not a list, data type -> \
{type(data_batch)}")
            data_f = self.filter_data(data_batch)
            # filter the data batch to only keep sensor data

            if (len(data_f) <= 0):
                raise Exception("Error data is empty, no valid data found")

            for data in data_f:
                float(data[1])
                self.sensor_report += 1
                if (data[0] == "temp"):
                    self.avg_t.append(data[1])

        except (Exception, ValueError) as e:
            print(e)
            return "0 readings"
        else:
            return f"{self.sensor_report} readings"

    def filter_data(self, data_batch: List[Union[tuple, str]],
                    criteria: Optional[str] = None) -> List[tuple]:
        """Garde uniquement les tuples capteurs valides, optionnellement prioritaires.

        Args:
            data_batch: Données sources à filtrer.
            criteria: Si "High-priority", isole les valeurs hors seuils.

        Returns:
            Liste de tuples (type, valeur) pour capteurs pris en charge.
        """
        filtered_data = [data for data in data_batch
                         if isinstance(data, tuple) is True
                         and data[0] in ["temp", "humidity", "presure"]]

        if (criteria == "High-priority"):
            return [data for data in filtered_data if
                    (data[0] == "temp" and (data[1] < -15 or data[1] > 35))
                    or (data[0] == "humidity"
                        and (data[1] < 10 or data[1] > 90))
                    or (data[0] == "presure"
                        and (data[1] < 1005 or data[1] > 1025))
                    ]
        return filtered_data

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Calcule la moyenne des températures collectées.

        Returns:
            {"average_temperature": float} — 0 si aucune mesure valide.
        """
        try:
            return {"average_temperature": sum(self.avg_t) / len(self.avg_t)}
        except ZeroDivisionError as e:
            print(e)
            return {"average_temperature": 0}


class TransactionStream(DataStream):
    """Flux dédié aux transactions financières (achats/ventes)."""
    def __init__(self, stream_id: str, type: str):
        """Initialise le flux transaction et ses compteurs.

        Args:
            stream_id: Identifiant unique du flux.
            type: Libellé du type de flux (ex. "Transaction").
        """
        super().__init__(stream_id, type)
        self.operation = 0
        self.net_flow = []

    def process_batch(self, data_batch: List[Any]) -> str:
        """Valide, filtre, puis comptabilise un lot de transactions.

        Les ventes sont comptées négativement dans le flux net, les achats
        positivement.

        Args:
            data_batch: Lot hétérogène de données.

        Returns:
            Chaîne de type "N operation" ou "0 readings" en cas d'erreur.
        """
        try:
            if (isinstance(data_batch, List) is False):
                raise Exception(f"Error data is not a list, data type -> \
{type(data_batch)}")
            data_f = self.filter_data(data_batch)
            # filter the data batch to only keep transaction data

            if (len(data_f) <= 0):
                raise Exception("Error data is empty, no valid data found")

            for data in data_f:
                int(data[1])
                self.operation += 1
                if (data[0] == "sell"):
                    self.net_flow.append(data[1] * -1)
                else:
                    self.net_flow.append(data[1])

        except (Exception, ValueError) as e:
            print(e)
            return "0 readings"
        else:
            return f"{self.operation} operation"

    def filter_data(self, data_batch: List[Union[tuple, str]],
                    criteria: Optional[str] = None) -> List[tuple]:
        """Garde uniquement les transactions valides.

        Args:
            data_batch: Données sources à filtrer.
            criteria: Si "High-priority", isole les gros achats.

        Returns:
            Liste de tuples (action, montant) pour "buy" ou "sell".
        """
        filtered_data = [data for data in data_batch
                         if isinstance(data, tuple) is True
                         and data[0] in ["buy", "sell"]]
        if (criteria == "High-priority"):
            return [data for data in filtered_data
                    if data[0] == "buy" and data[1] > 10000000]
        return filtered_data

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Calcule le flux net cumulé des transactions.

        Returns:
            {"net_flow": str} — avec signe explicite pour les positifs.
        """
        result = 0
        for operation in self.net_flow:
            result += operation
        if (result > 0):
            return {"net_flow": f"+{result}"}
        return {"net_flow": str(result)}


class EventStream(DataStream):
    """Flux d'événements systèmes (login/logout, erreurs, etc.)."""
    def __init__(self, stream_id: str, type: str):
        """Initialise le flux d'événements et ses compteurs.

        Args:
            stream_id: Identifiant unique du flux.
            type: Libellé du type de flux (ex. "Event").
        """
        super().__init__(stream_id, type)
        self.events = 0
        self.nb_errors = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        """Valide, filtre et compte les événements, dont les erreurs.

        Args:
            data_batch: Lot d'événements et autres données.

        Returns:
            Chaîne de type "N events" ou "0 readings" en cas d'erreur.
        """
        try:
            if (isinstance(data_batch, List) is False):
                raise Exception(f"Error data is not a list, data type -> \
{type(data_batch)}")
            data_f = self.filter_data(data_batch)
            # filter the data batch to only keep event data

            if (len(data_f) <= 0):
                raise Exception("Error data is empty, no valid data found")

            for data in data_f:
                self.events += 1
                if (data == "error"):
                    self.nb_errors += 1

        except (Exception, ValueError) as e:
            print(e)
            return "0 readings"
        else:
            return f"{self.events} events"

    def filter_data(self, data_batch: List[Union[tuple, str]],
                    criteria: Optional[str] = None) -> List[str]:
        """Garde uniquement les événements texte valides.

        Args:
            data_batch: Données sources à filtrer.
            criteria: Si "High-priority", ne conserve que "error".

        Returns:
            Liste de chaînes représentant des événements supportés.
        """
        filtered_data = [data for data in data_batch
                         if isinstance(data, str)
                         and data in ["error", "login",
                                      "logout", "emtpy_trash"]]

        if (criteria == "High-priority"):
            return [data for data in filtered_data if data == "error"]
        return (filtered_data)

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Retourne le nombre d'événements de type erreur rencontrés.

        Returns:
            {"error_count": int}
        """
        return {"error_count": self.nb_errors}


class StreamProcessor():
    """Orchestre le traitement polymorphe d'un lot sur plusieurs flux."""
    def process_batch(self, data_batch: List[Any],
                      streams: List[object]) -> None:
        """Applique `process_batch` de chaque flux puis affiche un résumé.

        Args:
            data_batch: Lot à transmettre à chaque flux.
            streams: Liste d'instances implémentant `process_batch`.
        """
        for stream in streams:
            result = stream.process_batch(data_batch)
            print(f"- {stream.type} data: {result} processed")

    def process_batch_filtered(self, data_batch: List[Any],
                               streams: List[Any],
                               criteria: str) -> Dict[str, int]:
        """Compte, par flux, les éléments correspondant à un critère.

        Args:
            data_batch: Lot à filtrer.
            streams: Flux cibles (doivent exposer `filter_data`).
            criteria: Nom du filtre (ex. "High-priority").

        Returns:
            Dictionnaire {type_flux: nombre_d'éléments_filtrés}.
        """
        liste = []
        for stream in streams:
            liste.append(len(stream.filter_data(data_batch, criteria)))

        return {key.type: value for key, value in zip(streams, liste)}


if (__name__ == "__main__"):
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    data_batch = [
                ("temp", 22.5), ("humidity", 65), ("presure", 1013),
                ("buy", 100), ("sell", 150), ("buy", 75),
                "login", "error", "logout"
                ]

    # data_batch = [
    #             1, 40, 2
    #             ]

    data_batch2 = [
                ("temp", -33), ("humidity", 20),
                ("buy", 100), ("sell", 150), ("buy", 15000000), ("buy", 1),
                "login", "emtpy_trash", "logout"
                ]

    data_batch3 = [
                ("temp", -33), ("humidity", 95), ("sell", 150),
                ("buy", 15000000), ("buy", 100), ("buy", 1),
                "login", "emtpy_trash", "logout"
                ]

    stream_type = [
                   SensorStream("SENSOR_002", "Sensor"),
                   TransactionStream("TRANS_002", "Transaction"),
                   EventStream("EVENT_001", "Event")
                   ]

    print("Initializing Sensor Stream...")
    sensor_stream = SensorStream("SENSOR_001", "Sensor")
    print(f"Stream ID: {sensor_stream.stream_id}, Type: Environmental Data")
    data_batch_filtered = sensor_stream.filter_data(data_batch)
    print(f"Processing sensor batch: \
[{''.join(f'{x}:{y}, ' for x, y in data_batch_filtered)}]")
    print(f"Sensor analysis: {sensor_stream.process_batch(data_batch)} \
processed, avg temp: {sensor_stream.get_stats()['average_temperature']}°C")

    print("\nInitializing Transaction Stream ...")
    transaction_stream = TransactionStream("TRANS_001", "Transaction")
    print(f"Stream ID: {transaction_stream.stream_id}, Type: Financial data")
    data_batch_filtered = transaction_stream.filter_data(data_batch)
    print(f"Processing transaction batch: \
[{''.join(f'{x}:{y}, ' for x, y in data_batch_filtered)}]")
    print(f"Transaction analysis: \
{transaction_stream.process_batch(data_batch)} \
processed, net flow: {transaction_stream.get_stats()['net_flow']} units")

    print("\nInitializing Event Stream...")
    event_stream = EventStream("EVENT_001", "Event")
    print(f"Stream ID: {event_stream.stream_id}, Type: System Events")
    data_batch_filtered = event_stream.filter_data(data_batch)
    print(f"Processing event batch: \
[{''.join(f'{event}, ' for event in data_batch_filtered)}]")
    print(f"Event analysis: {event_stream.process_batch(data_batch)} \
processed, {event_stream.get_stats()['error_count']} error detected")

    print("\n=== Polymorphic Stream Processing ===")
    print("processing mixed stream types through unified interface ...\n")
    print("Batch 1 Results:")
    StreamProcessor().process_batch(data_batch2, stream_type)

    print("\nSream filtering active: High-priority data only")
    filtered_result = StreamProcessor().process_batch_filtered(data_batch3,
                                                               stream_type,
                                                               "High-priority")
    print(f"Filtered results: {filtered_result['Sensor']} \
critical sensor alerts, {filtered_result['Transaction']} large transaction")
