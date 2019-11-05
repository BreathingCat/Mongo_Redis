__author__ = 'Hugo Bosque Arenas, Javier Garcia Rubio'

from pymongo import MongoClient
import redis
from uuid import uuid4
import random
from cryptography import fernet
from bson import ObjectId
import json
from django.contrib.gis.geos import Point, GeometryCollection


class ModelCursor:
    """ Cursor para iterar sobre los documentos del resultado de una
    consulta. Los documentos deben ser devueltos en forma de objetos
    modelo.
    """

    def __init__(self, model_class, command_cursor):
        """ Inicializa ModelCursor
        Argumentos:
            model_class (class) -- Clase para crear los modelos del
            documento que se itera.
            command_cursor (CommandCursor) -- Cursor de pymongo
        """
        self.model_class = model_class
        self.command_cursor = command_cursor

    def next(self):
        """ Devuelve el siguiente documento en forma de modelo
        """

        return self.model_class(**(self.command_cursor.next()))

    @property
    def alive(self):
        """True si existen más modelos por devolver, False en caso contrario
        """
        return self.command_cursor.alive


class Model:
    """ Prototipo de la clase modelo
        Copiar y pegar tantas veces como modelos se deseen crear (cambiando
        el nombre Model, por la entidad correspondiente), o bien crear tantas
        clases como modelos se deseen que hereden de esta clase. Este segundo
        metodo puede resultar mas compleja
    """
    required_vars = set()
    admissible_vars = set()
    db = None
    redis = None

    def __init__(self, **kwargs):
        if set(Model.required_vars) <= set(kwargs.keys()) <= set(Model.admissible_vars):
            self.__dict__.update(kwargs)
            self.updated_vars = set()

        else:
            raise Exception("ERROR INITIALIZING MODEL: kwargs did not match required and admissible vars")

    def save(self):
        temp = {key: self.__dict__[key] for key in self.updated_vars}
        Model.db.update({"_id": self._id}, {'$set': temp})
        self.updated_vars = []

    def update(self, **kwargs):
        if kwargs <= Model.admissible_vars:
            self.__dict__.update(kwargs)
            self.updated_vars.update(kwargs.keys())

        else:
            raise Exception("ERROR UPDATING MODEL: kwargs did not match admissible vars")

    @classmethod
    def query(cls, query):

        # Search item in Redis cache
        item = cls.redis.get(query)

        # If item was not found in cache, we query the main MongoDB
        if item is None:
            item = cls.db.find_one({'_id': ObjectId(query)})

            # If no element was found
            if item is None:
                return None

            # Update redis
            else:
                cls.redis.set(query, str(item))
                cls.redis.expire(query, 86400)

        else:
            # We need to parse the str using different brackets so that we can parse str to json
            item = item.replace("\'", "\"").replace("ObjectId(\"" + query + "\")", "\"ObjectId(\'" + query + "\')\"")
            item = json.loads(item)

        return Model(**item)

    @classmethod
    def init_class(cls, collection, vars_path):
        """ Inicializa las variables de clase en la inicializacion del sistema.
        Argumentos:
            collection (MongoClient) -- Conexion a la coleccion de la base de datos.
            vars_path (str) -- ruta al archivo con la definicion de variables
            del modelo.
        """

        cls.db = collection
        var_file = open(vars_path, 'r')
        json_vars = json.load(var_file)

        cls.db.create_index("ciudad")

        cls.required_vars = json_vars["required_vars"]
        cls.admissible_vars = json_vars["admissible_vars"]

        cls.redis = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)


def getCityGeoJSON(address):
    """ Devuelve las coordenadas de una direcciion a partir de un str de la direccion
    Argumentos:
        address (str) -- Direccion
    Return:
        (str) -- GeoJSON
    """
    from geopy.geocoders import Nominatim
    geolocator = Nominatim("P1_G7_Hugo_Bosque_Arenas-Javier_Garcia_Rubio.py")

    location = geolocator.geocode(address)

    gc = GeometryCollection(Point(location.latitude, location.longitude))

    return gc.geojson


class Session:

    key = None
    redis = None
    db = None

    def __init__(self):

        if Session.key is None or Session.redis is None or Session.db is None:
            raise Exception("ERROR: Decryption key is not initialized. Have you tried using init_class?")

            # self.password = fernet.Fernet(Session.key).encrypt(self.password.encode())

    @classmethod
    def init_class(cls, collection, key_path):
        """
        Stores key in class opening file specified by key_path
        :param key_path: Path of the file containing the key
        """

        cls.redis = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

        try:
            file = open(key_path, "rb")
            key = file.read()
            file.close()

            cls.key = key
            cls.db = collection

        except IOError:
            print("ERROR: File could not be opened")

        except:
            print("ERROR: Unclassified error ocurred")

    # def save(self):
    #     db.insert([{"name": self.__dict__["name"], "username": self.__dict__["username"], "password": self.__dict__["password"], "privileges": self.__dict__["privileges"]}], upsert=True)

    def login(self, username, password):
        token = Session.redis.get(username)

        # If login token does not exist, check username and password in mongoDB and generate a new one
        if token is None:

            result_set = self.db.find_one({'username': username})

            if result_set is None:
                print("ERROR: Username does not exist!")

            elif result_set["password"] is not password:
                print("ERROR: Incorrect password")

            else:
                token = str(uuid4())
                self.redis.set(username, token)
                self.redis.expire(username, 30*24*3600)

                return token, random.randint(0,10)

        # If login token already exists, return token and user privileges
        else:
            result_set = self.db.find_one({'username': username})
            return token, result_set["privileges"]


# # Q1: Listado de todas las personas de Huelva
# Q1 = [{'$match': {'ciudad': 'Huelva'}}];
# # Q2: Listado de todas personas que han estudiado en la UPM o UAM.
# Q2 = [{'$match': {'$or': [{'estudios.universidad': 'UPM'}, {'estudios.universidad': 'UAM'}]}}];
# # Q3: Listado de las diferentes ciudades en las que se encuentran las personas
# Q3 = [{'$group': {'Ciudad': {'$addToSet': 'ciudad'}}}, {'$proyect': {'Ciudad': 1}}];
# # Q4: Listado de las 10 personas más cercanas a unas coordenadas determinadas.
# Q4 = [{'$nearSphere': {'$geometry': {'type': 'Point', 'coordinates': [" , "]}}}, {'$limit': 10}];
# # Q5: Guarda en una tabla nueva el listado de las personas que ha terminado alguno de sus estudios en el 2017 o después.
# Q5 = [{'$unwind': '$estudios'},
#       {'$project': {'estudios': {'universidad': 1, 'final': 1, 'inicio': {
#           '$dateFromString': {'dateString': '$estudios.inicio', 'format': '%d/%m/%Y'}}}, 'nombre': 1, 'telefonos': 1,
#                     'ciudad': 1, 'trabajo': 1}}, {'$match': {'estudios.inicio': {'$gte': 'ISODate(2017-01-01)'}}},
#       {'$group': {'_id': '$_id', 'nombre': {'$first': '$nombre'}, 'trabajo': {'$first': '$trabajo'},
#                   'ciudad': {'$first': '$ciudad'}, 'estudios': {'$push': '$estudios'}}}];
# # Q6: Calcular el número medio de estudios realizados por las personas que han trabajado o trabajan en la UPM.
# Q6 = [{'$match': {'trabajo.empresa': 'UPM'}},
#       {'$group': {'_id': '', 'media_estudios': {'$avg': {'$size': '$trabajo'}}}}];
# # Q7: Listado de las tres universidades que más veces aparece como centro de estudios de las personas registradas. Mostrar universidad y el número de veces que aparece.
# Q7 = [{"$unwind": "$estudios"}, {"$group": {"_id": "$estudios.universidad", "numberOfStudents": {"$sum": 1}}},
#       {"$sort": {"numberOfStudents": -1}}, {"$limit": 3}];


if __name__ == '__main__':
    client = MongoClient()

    db = client["P1"]
    collection = db["zips"]
    Model.init_class(collection, "/home/hugo/Mongo_Project/vars.txt")

    db1 = client["UsersP2"]
    collection1 = db1["zips"]
    Session.init_class(collection1, "./keys.keys")

    user = Session()
    user.login("Admin","password")

    # decrypt_pass = fernet.Fernet(User.key).decrypt(user.password).decode("utf-8")

    # var = Model.query("5da1bcbfbdaf2e265d79ea78")

    # """while True:
    #    try:
    #        print(var.next().__dict__)
    #
    #    except:
    #        print("No more elements in ResultSet")
    #        break"""
